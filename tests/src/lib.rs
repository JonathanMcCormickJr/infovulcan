#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]
// E2E tests are long, sequential service-interaction scripts; the line count is inherent.
#![allow(clippy::too_many_lines)]

//! Integration / end-to-end test harness spanning multiple `InfoVulcan` services.
//!
//! This crate has no library surface of its own; the tests live in the modules below and spin up
//! real services to exercise cross-service flows.

#[cfg(test)]
mod e2e;
