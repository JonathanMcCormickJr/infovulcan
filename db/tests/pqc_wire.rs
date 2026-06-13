//! End-to-end demonstration of the app-layer post-quantum wire wrap over real gRPC.
//!
//! A payload is sealed with `proto::pqc` (Kyber-768 KEM) and transmitted through a real DB
//! `Put`/`Get` round-trip. The bytes stored/transmitted are opaque Kyber ciphertext — the DB
//! never sees the plaintext — and only the holder of the private key can recover it. In
//! production this rides *inside* the mTLS tunnel (see `db/tests/mtls.rs`), giving the
//! double-layer (TLS 1.3 + Kyber) confidentiality from ARCHITECTURE.md.

use std::sync::Arc;

use db::network::DbNetworkFactory;
use db::raft::{DbRaft, DbStore};
use db::server::DatabaseService;
use db::server::db::database_client::DatabaseClient;
use db::server::db::database_server::DatabaseServer;
use db::server::db::{GetRequest, PutRequest};
use openraft::Config;
use openraft::storage::Adaptor;
use shared::encryption::EncryptionService;
use tonic::transport::Server;

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
async fn kyber_sealed_payload_survives_a_grpc_round_trip() {
    // Recipient (the reader) owns the Kyber keypair.
    let (public_key, private_key) = EncryptionService::generate_keypair().unwrap();
    let plaintext = b"top-secret session token that must survive a quantum adversary";

    // Sender seals to the recipient's public key.
    let sealed = proto::pqc::seal(plaintext, &public_key).unwrap();
    assert_ne!(sealed.as_slice(), plaintext.as_slice());

    // Serve a real DB and transmit the sealed payload over gRPC.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let svc = make_db_service().await;
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(DatabaseServer::new(svc))
            .serve(addr)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let mut client = DatabaseClient::connect(format!("http://{}:{}", addr.ip(), addr.port()))
        .await
        .expect("connect");

    client
        .put(PutRequest {
            collection: "audit".to_string(),
            key: b"sealed-1".to_vec(),
            value: sealed.clone(),
        })
        .await
        .expect("put sealed payload");

    let got = client
        .get(GetRequest {
            collection: "audit".to_string(),
            key: b"sealed-1".to_vec(),
        })
        .await
        .expect("get")
        .into_inner();
    assert!(got.found);
    // What travelled the wire / sits in storage is the Kyber ciphertext, not the plaintext.
    assert_eq!(got.value, sealed);
    assert!(!got.value.windows(plaintext.len()).any(|w| w == plaintext));

    // Only the private-key holder can recover the plaintext.
    let recovered = proto::pqc::open(&got.value, &private_key).unwrap();
    assert_eq!(recovered, plaintext);

    server.abort();
}
