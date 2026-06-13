//! mTLS configuration for internal gRPC.
//!
//! Every internal service authenticates to every other with a service-unique certificate
//! signed by a shared internal CA (mutual TLS over rustls / TLS 1.3). TLS is **opt-in**: it
//! is enabled only when the `TLS_CA_CERT`, `TLS_CERT`, and `TLS_KEY` environment variables are
//! all set (pointing at PEM files). With them unset, services serve and dial in plaintext —
//! the dev/test/demo default — so nothing requires certificates to run locally.
//!
//! Generate a dev PKI with `scripts/gen-certs.sh` (CA + one cert per service, SANs covering
//! `localhost`/`127.0.0.1`). Set `TLS_DOMAIN` (default `localhost`) to the name the client
//! should verify against the peer certificate.

use anyhow::{Context, Result};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

/// Path to the internal CA certificate (trust root for verifying peers).
pub const ENV_CA: &str = "TLS_CA_CERT";
/// Path to this service's certificate (presented to peers).
pub const ENV_CERT: &str = "TLS_CERT";
/// Path to this service's private key.
pub const ENV_KEY: &str = "TLS_KEY";
/// Domain name a client verifies against the server certificate (default `localhost`).
pub const ENV_DOMAIN: &str = "TLS_DOMAIN";

fn read(path: &str) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading TLS file {path}"))
}

/// Whether mTLS is configured (all of CA/CERT/KEY env vars are set).
#[must_use]
pub fn enabled() -> bool {
    std::env::var(ENV_CA).is_ok()
        && std::env::var(ENV_CERT).is_ok()
        && std::env::var(ENV_KEY).is_ok()
}

/// Build a mutual-TLS [`ServerTlsConfig`] from in-memory PEMs: presents `cert`/`key` and
/// requires client certificates signed by `ca_pem`.
#[must_use]
pub fn server_config_from_pem(ca_pem: &str, cert_pem: &str, key_pem: &str) -> ServerTlsConfig {
    ServerTlsConfig::new()
        .identity(Identity::from_pem(cert_pem, key_pem))
        .client_ca_root(Certificate::from_pem(ca_pem))
}

/// Build a mutual-TLS [`ClientTlsConfig`] from in-memory PEMs: presents `cert`/`key`, trusts
/// `ca_pem`, and verifies the server certificate against `domain`.
#[must_use]
pub fn client_config_from_pem(
    ca_pem: &str,
    cert_pem: &str,
    key_pem: &str,
    domain: &str,
) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .domain_name(domain.to_string())
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
}

/// Server TLS config from the environment, or `None` if TLS is not configured.
///
/// # Errors
///
/// Returns an error if TLS is configured but a PEM file cannot be read.
pub fn server_config() -> Result<Option<ServerTlsConfig>> {
    if !enabled() {
        return Ok(None);
    }
    let ca = read(&std::env::var(ENV_CA)?)?;
    let cert = read(&std::env::var(ENV_CERT)?)?;
    let key = read(&std::env::var(ENV_KEY)?)?;
    Ok(Some(server_config_from_pem(&ca, &cert, &key)))
}

/// Client TLS config from the environment, or `None` if TLS is not configured.
///
/// # Errors
///
/// Returns an error if TLS is configured but a PEM file cannot be read.
pub fn client_config() -> Result<Option<ClientTlsConfig>> {
    if !enabled() {
        return Ok(None);
    }
    let ca = read(&std::env::var(ENV_CA)?)?;
    let cert = read(&std::env::var(ENV_CERT)?)?;
    let key = read(&std::env::var(ENV_KEY)?)?;
    let domain = std::env::var(ENV_DOMAIN).unwrap_or_else(|_| "localhost".to_string());
    Ok(Some(client_config_from_pem(&ca, &cert, &key, &domain)))
}

/// Apply server mTLS to a [`Server`] builder if configured (otherwise return it unchanged).
///
/// # Errors
///
/// Returns an error if the TLS config is invalid or a PEM file cannot be read.
pub fn apply_server_tls(server: Server) -> Result<Server> {
    match server_config()? {
        Some(tls) => Ok(server.tls_config(tls)?),
        None => Ok(server),
    }
}

fn to_https(addr: &str) -> String {
    addr.strip_prefix("http://")
        .map_or_else(|| addr.to_string(), |rest| format!("https://{rest}"))
}

/// Build an [`Endpoint`] for `addr` with client mTLS applied if configured. When TLS is on, an
/// `http://` address is rewritten to `https://`.
///
/// # Errors
///
/// Returns an error if the address is invalid or the TLS config cannot be built.
pub fn endpoint(addr: &str) -> Result<Endpoint> {
    match client_config()? {
        Some(tls) => {
            let endpoint = Endpoint::from_shared(to_https(addr))
                .context("invalid endpoint uri")?
                .tls_config(tls)?;
            Ok(endpoint)
        }
        None => Ok(Endpoint::from_shared(addr.to_string()).context("invalid endpoint uri")?),
    }
}

/// Connect a [`tonic::transport::Channel`] to `addr`, using client mTLS if configured.
///
/// # Errors
///
/// Returns an error if the connection cannot be established.
pub async fn connect(addr: &str) -> Result<tonic::transport::Channel> {
    Ok(endpoint(addr)?.connect().await?)
}

/// Build a lazily-connecting [`tonic::transport::Channel`] to `addr` (mTLS if configured).
///
/// # Errors
///
/// Returns an error if the endpoint cannot be built.
pub fn connect_lazy(addr: &str) -> Result<tonic::transport::Channel> {
    Ok(endpoint(addr)?.connect_lazy())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed cert/key PEM pair (valid PEM is all the tonic builders need at construction).
    fn self_signed_pem() -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        (cert.pem(), key.serialize_pem())
    }

    #[test]
    fn enabled_is_false_without_env() {
        // The test environment has no TLS_* vars set.
        assert!(!enabled());
    }

    #[test]
    fn env_configs_are_none_when_disabled() {
        assert!(server_config().unwrap().is_none());
        assert!(client_config().unwrap().is_none());
    }

    #[test]
    fn apply_server_tls_is_passthrough_when_disabled() {
        // Should return the builder unchanged without error.
        let _server = apply_server_tls(Server::builder()).unwrap();
    }

    #[test]
    fn builds_server_and_client_configs_from_pem() {
        let (cert, key) = self_signed_pem();
        // ca_pem can be any valid cert PEM for construction purposes.
        let _s = server_config_from_pem(&cert, &cert, &key);
        let _c = client_config_from_pem(&cert, &cert, &key, "localhost");
    }

    #[test]
    fn to_https_only_rewrites_http_scheme() {
        assert_eq!(to_https("http://db:50051"), "https://db:50051");
        assert_eq!(to_https("https://db:50051"), "https://db:50051");
        // No scheme prefix → left untouched.
        assert_eq!(to_https("db:50051"), "db:50051");
    }

    #[test]
    fn endpoint_builds_in_plaintext() {
        // TLS disabled → endpoint keeps the http scheme and builds without error.
        let _ep = endpoint("http://127.0.0.1:50051").unwrap();
    }

    #[tokio::test]
    async fn lazy_connect_builds_a_channel() {
        // connect_lazy needs a Tokio reactor to construct its connector.
        let _ch = connect_lazy("http://127.0.0.1:50051").unwrap();
    }

    #[test]
    fn endpoint_rejects_a_malformed_uri() {
        assert!(endpoint("::::not a uri::::").is_err());
    }
}
