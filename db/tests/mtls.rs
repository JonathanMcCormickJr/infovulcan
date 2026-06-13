//! End-to-end mTLS test for internal gRPC.
//!
//! Builds a one-off PKI (CA + server cert + client cert) in memory with `rcgen`, serves the
//! real `DatabaseService` over `proto::tls`'s mutual-TLS server config, and asserts:
//!   - a client presenting a CA-signed identity completes an RPC, and
//!   - a client with no client identity is rejected by the mutual handshake.

use std::sync::Arc;
use std::time::Duration;

use db::network::DbNetworkFactory;
use db::raft::{DbRaft, DbStore};
use db::server::DatabaseService;
use db::server::db::HealthRequest;
use db::server::db::database_client::DatabaseClient;
use db::server::db::database_server::DatabaseServer;
use openraft::Config;
use openraft::storage::Adaptor;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Server};

struct Pem {
    cert: String,
    key: String,
}

struct Pki {
    ca: String,
    server: Pem,
    client: Pem,
}

fn make_ca() -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "infovulcan-test-ca");
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    let key = KeyPair::generate().unwrap();
    CertifiedIssuer::self_signed(params, key).unwrap()
}

fn make_leaf(cn: &str, eku: ExtendedKeyUsagePurpose, issuer: &CertifiedIssuer<'_, KeyPair>) -> Pem {
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.extended_key_usages.push(eku);
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, issuer).unwrap();
    Pem {
        cert: cert.pem(),
        key: key.serialize_pem(),
    }
}

fn build_pki() -> Pki {
    let ca = make_ca();
    Pki {
        server: make_leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth, &ca),
        client: make_leaf("db-client", ExtendedKeyUsagePurpose::ClientAuth, &ca),
        ca: ca.pem(),
    }
}

async fn make_db_service() -> DatabaseService {
    let store = DbStore::new_temp().unwrap();
    let cfg = Arc::new(Config::default().validate().unwrap());
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let raft = DbRaft::new(1, cfg, DbNetworkFactory::new(), log_store, state_machine)
        .await
        .unwrap();
    let mut members = std::collections::BTreeSet::new();
    members.insert(1);
    let _ = raft.initialize(members).await;
    let storage = store.state_machine().read().await.storage.clone();
    DatabaseService::new(raft, storage)
}

#[tokio::test]
async fn mtls_client_with_identity_succeeds_and_without_is_rejected() {
    let pki = build_pki();

    // Reserve an ephemeral port, then serve the real DB service over mutual TLS.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server_tls = proto::tls::server_config_from_pem(&pki.ca, &pki.server.cert, &pki.server.key);
    let svc = make_db_service().await;
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(server_tls)
            .expect("server tls")
            .add_service(DatabaseServer::new(svc))
            .serve(addr)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let uri = format!("https://localhost:{}", addr.port());

    // 1. A client presenting a CA-signed identity completes an RPC.
    let client_tls =
        proto::tls::client_config_from_pem(&pki.ca, &pki.client.cert, &pki.client.key, "localhost");
    let channel = Endpoint::from_shared(uri.clone())
        .unwrap()
        .tls_config(client_tls)
        .unwrap()
        .connect()
        .await
        .expect("mTLS client with identity should connect");
    let mut client = DatabaseClient::new(channel);
    let health = client
        .health(HealthRequest {})
        .await
        .expect("health RPC over mTLS")
        .into_inner();
    assert_eq!(health.node_id, "1");

    // 2. A client that trusts the CA but presents NO identity is rejected by mutual TLS.
    let no_identity = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(&pki.ca));
    let rejected = match Endpoint::from_shared(uri)
        .unwrap()
        .tls_config(no_identity)
        .unwrap()
        .connect()
        .await
    {
        Err(_) => true,
        Ok(ch) => {
            // Some stacks defer the failure to the first request.
            DatabaseClient::new(ch)
                .health(HealthRequest {})
                .await
                .is_err()
        }
    };
    assert!(
        rejected,
        "client without a client cert must be rejected by mTLS"
    );

    server.abort();
}
