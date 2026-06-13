#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]

//! Shared types and utilities for the `InfoVulcan` ticketing system.
//!
//! This crate provides common data structures, enums, and traits used across
//! all `InfoVulcan` microservices. It ensures type consistency and enables seamless
//! serialization/deserialization for both gRPC and REST APIs.

pub mod discovery;
pub mod encryption;
pub mod error;
pub mod key_store;
pub mod ticket;
pub mod user;

// Re-export commonly used types
pub use discovery::ServiceRegistry;
pub use encryption::{EncryptedData, EncryptionAlgorithm, EncryptionError, EncryptionService};
pub use error::InfoVulcanError;
pub use ticket::{NextAction, Resolution, Symptom, Ticket, TicketPriority, TicketStatus};
pub use user::{AuthMethod, Role, User};
