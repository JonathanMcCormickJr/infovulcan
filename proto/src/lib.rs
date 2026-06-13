#![allow(clippy::all, clippy::pedantic)]

pub mod admin {
    tonic::include_proto!("admin");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod chaos {
    tonic::include_proto!("chaos");
}

pub mod custodian {
    tonic::include_proto!("custodian");
}

pub mod db {
    tonic::include_proto!("db");
}

pub mod honeypot {
    tonic::include_proto!("honeypot");
}

#[allow(clippy::doc_markdown)]
pub mod tls;

#[allow(clippy::doc_markdown)]
pub mod pqc;
