//! Exercises the environment-driven branches of `proto::tls` (the `enabled()==true` paths that
//! read PEM files and build mutual-TLS configs). Kept in its own integration binary so the
//! process-global `TLS_*` env mutation here cannot race the in-crate unit tests that assert the
//! *disabled* default.

use proto::tls;

/// Write a self-signed cert/key to `dir` and return their paths (used as CA, cert, and key).
fn write_pki(dir: &std::path::Path) -> (String, String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let ca_path = dir.join("ca.crt");
    let cert_path = dir.join("svc.crt");
    let key_path = dir.join("svc.key");
    std::fs::write(&ca_path, cert.pem()).unwrap();
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (
        ca_path.to_str().unwrap().to_string(),
        cert_path.to_str().unwrap().to_string(),
        key_path.to_str().unwrap().to_string(),
    )
}

#[tokio::test]
async fn env_driven_tls_paths() {
    let dir = tempfile::tempdir().unwrap();
    let (ca, cert, key) = write_pki(dir.path());

    // SAFETY: single test in its own integration binary; no other thread reads these vars here.
    unsafe {
        std::env::set_var(tls::ENV_CA, &ca);
        std::env::set_var(tls::ENV_CERT, &cert);
        std::env::set_var(tls::ENV_KEY, &key);
        std::env::set_var(tls::ENV_DOMAIN, "localhost");
    }

    assert!(tls::enabled());
    assert!(tls::server_config().unwrap().is_some());
    assert!(tls::client_config().unwrap().is_some());

    // apply_server_tls should take the configured branch without error.
    let _server = tls::apply_server_tls(tonic::transport::Server::builder()).unwrap();

    // endpoint() should take the TLS branch and rewrite http→https; connect_lazy builds a channel.
    let _ep = tls::endpoint("http://localhost:50051").unwrap();
    let _ch = tls::connect_lazy("http://localhost:50051").unwrap();

    // A read failure surfaces as an error when a configured file is missing.
    unsafe { std::env::set_var(tls::ENV_CERT, dir.path().join("missing.crt")) };
    assert!(tls::server_config().is_err());

    unsafe {
        std::env::remove_var(tls::ENV_CA);
        std::env::remove_var(tls::ENV_CERT);
        std::env::remove_var(tls::ENV_KEY);
        std::env::remove_var(tls::ENV_DOMAIN);
    }
}
