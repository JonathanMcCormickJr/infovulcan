#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Honeypot service: deceptive high-value-target endpoints that capture and report intrusion
//! attempts to the admin service. All responses are synthetic; no real data is touched.

pub mod reporter;
pub mod service;
pub mod traps;
