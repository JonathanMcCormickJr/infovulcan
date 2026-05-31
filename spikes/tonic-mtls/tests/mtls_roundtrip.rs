//! Integration test for the mTLS spike.
//!
//! Builds a one-off PKI (CA + server cert + client cert) in memory using
//! `rcgen`, spins up the tonic Echo server with `ServerTlsConfig` requiring a
//! client cert signed by that CA, and runs an Echo round-trip from a tonic
//! client that presents the matching client cert.
//!
//! Also asserts the negative case: a client that connects with no identity
//! is rejected at the TLS handshake — proving the "mutual" half of mTLS.

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use spike_tonic_mtls::EchoService;
use spike_tonic_mtls::echo::SayRequest;
use spike_tonic_mtls::echo::echo_client::EchoClient;
use spike_tonic_mtls::echo::echo_server::EchoServer;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

struct Pem {
    cert: String,
    key: String,
}

struct Pki {
    ca_cert_pem: String,
    server: Pem,
    client: Pem,
}

fn make_ca() -> Result<CertifiedIssuer<'static, KeyPair>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "spike-mtls-ca");
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::CrlSign);

    let key = KeyPair::generate()?;
    Ok(CertifiedIssuer::self_signed(params, key)?)
}

fn make_leaf(
    common_name: &str,
    sans: Vec<String>,
    eku: ExtendedKeyUsagePurpose,
    issuer: &CertifiedIssuer<'_, KeyPair>,
) -> Result<Pem> {
    let mut params = CertificateParams::new(sans)?;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.extended_key_usages.push(eku);

    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, issuer)?;
    Ok(Pem {
        cert: cert.pem(),
        key: key.serialize_pem(),
    })
}

fn build_pki() -> Result<Pki> {
    let ca = make_ca()?;
    let server = make_leaf(
        "localhost",
        vec!["localhost".to_string()],
        ExtendedKeyUsagePurpose::ServerAuth,
        &ca,
    )?;
    let client = make_leaf(
        "spike-client",
        vec!["spike-client".to_string()],
        ExtendedKeyUsagePurpose::ClientAuth,
        &ca,
    )?;
    Ok(Pki {
        ca_cert_pem: ca.pem(),
        server,
        client,
    })
}

/// Spawn the server bound to an ephemeral port and return the address it
/// landed on (so the client can dial it without races).
async fn spawn_server(pki: &Pki, shutdown: oneshot::Receiver<()>) -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_identity = Identity::from_pem(&pki.server.cert, &pki.server.key);
    let client_ca = Certificate::from_pem(&pki.ca_cert_pem);

    let tls = ServerTlsConfig::new()
        .identity(server_identity)
        .client_ca_root(client_ca);

    let incoming = TcpListenerStream::new(listener);
    let svc = EchoServer::new(EchoService);

    tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .expect("server TLS config valid")
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown.await;
            })
            .await
            .expect("server runs");
    });

    // Give the listener a moment to actually start accepting. Without this
    // the very first connect can race the spawn.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(addr)
}

fn client_endpoint(addr: SocketAddr, tls: ClientTlsConfig) -> Result<Endpoint> {
    let uri = format!("https://localhost:{}", addr.port());
    let endpoint = Endpoint::from_shared(uri).context("endpoint uri")?;
    endpoint.tls_config(tls).context("client TLS config")
}

#[tokio::test]
async fn mtls_echo_roundtrip() -> Result<()> {
    let pki = build_pki()?;
    let (tx, rx) = oneshot::channel();
    let addr = spawn_server(&pki, rx).await?;

    let client_identity = Identity::from_pem(&pki.client.cert, &pki.client.key);
    let server_ca = Certificate::from_pem(&pki.ca_cert_pem);
    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(server_ca)
        .identity(client_identity);

    let channel = client_endpoint(addr, tls)?.connect().await?;
    let mut client = EchoClient::new(channel);

    let reply = client
        .say(SayRequest {
            text: "hello over mtls".to_string(),
        })
        .await?
        .into_inner();
    assert_eq!(reply.text, "hello over mtls");

    let _ = tx.send(());
    Ok(())
}

#[tokio::test]
async fn server_rejects_client_without_identity() -> Result<()> {
    let pki = build_pki()?;
    let (tx, rx) = oneshot::channel();
    let addr = spawn_server(&pki, rx).await?;

    // Trust the server's CA but present no client identity. The server
    // requires a client cert, so the TLS handshake (or the first RPC) must
    // fail.
    let server_ca = Certificate::from_pem(&pki.ca_cert_pem);
    let tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(server_ca);

    let endpoint = client_endpoint(addr, tls)?;
    let failed = match endpoint.connect().await {
        Err(_) => true,
        Ok(channel) => {
            // Some TLS impls defer the failure to the first byte exchanged,
            // so also try an actual RPC.
            let mut client = EchoClient::new(channel);
            client
                .say(SayRequest {
                    text: "should not arrive".to_string(),
                })
                .await
                .is_err()
        }
    };
    assert!(failed, "client without identity must be rejected by mTLS");

    let _ = tx.send(());
    Ok(())
}
