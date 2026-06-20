#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Test-only helpers shared across `InfoVulcan` crates.
//!
//! Pulled in as a `dev-dependency`. The goal is to remove the boilerplate that every test module
//! re-implements when it needs to stand up a real in-process gRPC server to exercise a client
//! against.

#[doc(hidden)]
pub use tokio::sync::oneshot;

/// Stand up an already-wrapped tonic service on an ephemeral localhost port with graceful
/// shutdown, returning `(addr, shutdown_tx)`.
///
/// Pass the *wrapped* service (e.g. `AuthServiceServer::new(mock)`); the macro binds a free
/// `127.0.0.1` port, spawns the server on the current Tokio runtime, and hands back the bound
/// [`std::net::SocketAddr`] plus a [`oneshot::Sender`] — send `()` (or drop it) to stop the server.
///
/// Expanding at the call site (rather than via a generic fn) keeps callers free of tonic's
/// verbose `Service`/`NamedService` trait bounds while still sharing the bind/spawn logic.
///
/// The caller's crate must have `tonic` and `tokio` available (every test crate here does).
///
/// ```ignore
/// let (addr, shutdown) = test_support::spawn_grpc!(AuthServiceServer::new(MockAuth::default()));
/// // ... connect a client to `addr`, run assertions ...
/// let _ = shutdown.send(());
/// ```
#[macro_export]
macro_rules! spawn_grpc {
    ($service:expr) => {{
        let listener = ::std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = $crate::oneshot::channel::<()>();
        ::tokio::spawn(async move {
            let _ = ::tonic::transport::Server::builder()
                .add_service($service)
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }};
}
