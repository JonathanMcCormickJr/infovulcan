#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Spike crate: tonic + rustls mutual TLS.
//!
//! Exposes the generated `echo` gRPC types plus a trivial `EchoService`
//! implementation. The mTLS plumbing lives in the integration tests under
//! `tests/`, where it can manipulate certs freely without polluting the
//! library surface.

pub mod echo {
    #![allow(clippy::all, clippy::pedantic)]
    tonic::include_proto!("echo");
}

use echo::echo_server::Echo;
use echo::{SayReply, SayRequest};
use tonic::{Request, Response, Status};

#[derive(Debug, Default, Clone)]
pub struct EchoService;

#[tonic::async_trait]
impl Echo for EchoService {
    async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        let text = request.into_inner().text;
        Ok(Response::new(SayReply { text }))
    }
}
