//! Ticket types and enums for the `InfoVulcan` system.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// MAC address type with validation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MacAddress(String);

impl MacAddress {
    /// Create a new MAC address with validation
    ///
    /// # Errors
    ///
    /// Returns an error if the MAC address format is invalid
    pub fn new(mac: &str) -> Result<Self, crate::error::InfoVulcanError> {
        // Validate RFC-compliant MAC address format (XX:XX:XX:XX:XX:XX or XX-XX-XX-XX-XX-XX)
        let normalized = mac.replace('-', ":");
        let parts: Vec<&str> = normalized.split(':').collect();

        if parts.len() != 6 {
            return Err(crate::error::InfoVulcanError::ValidationError(format!(
                "Invalid MAC address format: {mac}"
            )));
        }

        for part in parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(crate::error::InfoVulcanError::ValidationError(format!(
                    "Invalid MAC address format: {mac}"
                )));
            }
        }

        Ok(Self(normalized.to_uppercase()))
    }

    /// Get the MAC address as a string
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MacAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Network device types supported by the company
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum NetworkDevice {
    DslModem {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 0,
    CoaxModem {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 1,
    Ont {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 2,
    FixedWirelessAntenna {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 3,
    VpnGw {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 4,
    Switch {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 5,
    Router {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 6,
    Firewall {
        make: String,
        model: String,
        mac_address: Option<MacAddress>,
        serial_number: Option<String>,
    } = 7,
}

impl NetworkDevice {
    /// Get the device type name
    #[must_use]
    pub const fn device_type(&self) -> &'static str {
        match self {
            Self::DslModem { .. } => "DSL Modem",
            Self::CoaxModem { .. } => "Coax Modem",
            Self::Ont { .. } => "ONT",
            Self::FixedWirelessAntenna { .. } => "Fixed Wireless Antenna",
            Self::VpnGw { .. } => "VPN Gateway",
            Self::Switch { .. } => "Switch",
            Self::Router { .. } => "Router",
            Self::Firewall { .. } => "Firewall",
        }
    }

    /// Get the device make and model as a formatted string
    #[must_use]
    pub fn make_model(&self) -> String {
        match self {
            Self::DslModem { make, model, .. }
            | Self::CoaxModem { make, model, .. }
            | Self::Ont { make, model, .. }
            | Self::FixedWirelessAntenna { make, model, .. }
            | Self::VpnGw { make, model, .. }
            | Self::Switch { make, model, .. }
            | Self::Router { make, model, .. }
            | Self::Firewall { make, model, .. } => format!("{make} {model}"),
        }
    }

    /// Get the MAC address if available
    #[must_use]
    pub const fn mac_address(&self) -> Option<&MacAddress> {
        match self {
            Self::DslModem { mac_address, .. }
            | Self::CoaxModem { mac_address, .. }
            | Self::Ont { mac_address, .. }
            | Self::FixedWirelessAntenna { mac_address, .. }
            | Self::VpnGw { mac_address, .. }
            | Self::Switch { mac_address, .. }
            | Self::Router { mac_address, .. }
            | Self::Firewall { mac_address, .. } => mac_address.as_ref(),
        }
    }
}

/// Unique ticket identifier (auto-incremented)
pub type TicketId = u64;

/// Primary symptom categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum Symptom {
    Unknown = 0,
    BroadbandDown = 1,
    BroadbandIntermittent = 2,
    PacketLoss = 3,
    Power = 4,
    VpnIssue = 5,
    ConfigurationError = 6,
    HardwareFailure = 7,
    SoftwareBug = 8,
    SecurityIncident = 9,
    SlowBandwidth = 10,
    DuplexingMismatch = 11,
    LatencyIssues = 12,
    JitterProblems = 13,
    DnsIssues = 14,
    Other = 255,
}

impl Symptom {
    #[must_use]
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::BroadbandDown,
            2 => Self::BroadbandIntermittent,
            3 => Self::PacketLoss,
            4 => Self::Power,
            5 => Self::VpnIssue,
            6 => Self::ConfigurationError,
            7 => Self::HardwareFailure,
            8 => Self::SoftwareBug,
            9 => Self::SecurityIncident,
            10 => Self::SlowBandwidth,
            11 => Self::DuplexingMismatch,
            12 => Self::LatencyIssues,
            13 => Self::JitterProblems,
            14 => Self::DnsIssues,
            255 => Self::Other,
            _ => Self::Unknown,
        }
    }
}

/// Ticket priority: how much impact is the customer feeling?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum TicketPriority {
    Unknown = 0,
    HardDown = 1,
    PrimaryDown = 2,
    BackupDown = 3,
    Intermittent = 4,
    PacketLoss = 5,
}

impl TicketPriority {
    #[must_use]
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::HardDown,
            2 => Self::PrimaryDown,
            3 => Self::BackupDown,
            4 => Self::Intermittent,
            5 => Self::PacketLoss,
            _ => Self::Unknown,
        }
    }
}

/// Ticket status workflow states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum TicketStatus {
    Open = 0,
    AwaitingCustomer = 1,
    AwaitingISP = 2,
    AwaitingPartner = 3,
    SupportHold = 4,
    HandedOff = 5,
    AppointmentScheduled = 6,
    EbondReceived = 7,
    VoicemailReceived = 8,
    AutoClose = 254,
    Closed = 255,
}

impl TicketStatus {
    #[must_use]
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::AwaitingCustomer,
            2 => Self::AwaitingISP,
            3 => Self::AwaitingPartner,
            4 => Self::SupportHold,
            5 => Self::HandedOff,
            6 => Self::AppointmentScheduled,
            7 => Self::EbondReceived,
            8 => Self::VoicemailReceived,
            254 => Self::AutoClose,
            255 => Self::Closed,
            _ => Self::Open,
        }
    }

    /// Check if this status requires a resolution
    #[must_use]
    pub const fn requires_resolution(self) -> bool {
        matches!(self, Self::Closed | Self::AutoClose)
    }

    /// Check if this status requires a next action
    #[must_use]
    pub const fn requires_next_action(self) -> bool {
        matches!(
            self,
            Self::Open | Self::AwaitingCustomer | Self::AwaitingISP | Self::AwaitingPartner
        )
    }

    /// Whether this is a terminal status (a lifecycle sink with no outgoing transitions).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::AutoClose)
    }

    /// Policy-as-code: the statuses a ticket may move **to** from `self`.
    ///
    /// This is the single source of truth for the ticket lifecycle state machine; tighten
    /// or loosen the workflow by editing these allow-lists. Terminal states return an empty
    /// slice. A no-op (`self -> self`) is always permitted and is handled by
    /// [`TicketStatus::can_transition_to`], not encoded here.
    #[must_use]
    pub fn allowed_transitions(self) -> &'static [TicketStatus] {
        use TicketStatus::{
            AppointmentScheduled, AutoClose, AwaitingCustomer, AwaitingISP, AwaitingPartner,
            Closed, EbondReceived, HandedOff, Open, SupportHold, VoicemailReceived,
        };
        match self {
            Open => &[
                AwaitingCustomer,
                AwaitingISP,
                AwaitingPartner,
                SupportHold,
                HandedOff,
                AppointmentScheduled,
                EbondReceived,
                VoicemailReceived,
                Closed,
                AutoClose,
            ],
            AwaitingCustomer => &[
                Open,
                SupportHold,
                AppointmentScheduled,
                EbondReceived,
                VoicemailReceived,
                Closed,
                AutoClose,
            ],
            AwaitingISP => &[Open, SupportHold, EbondReceived, Closed, AutoClose],
            AwaitingPartner => &[Open, SupportHold, HandedOff, Closed, AutoClose],
            SupportHold => &[
                Open,
                AwaitingCustomer,
                AwaitingISP,
                AwaitingPartner,
                AppointmentScheduled,
                Closed,
                AutoClose,
            ],
            HandedOff => &[Open, AwaitingPartner, Closed, AutoClose],
            AppointmentScheduled => &[Open, AwaitingCustomer, SupportHold, Closed, AutoClose],
            EbondReceived => &[Open, AwaitingISP, Closed, AutoClose],
            VoicemailReceived => &[Open, AwaitingCustomer, Closed, AutoClose],
            // Terminal states and any future variant: no outgoing transitions.
            _ => &[],
        }
    }

    /// Policy-as-code check: whether a ticket may transition from `self` to `to`.
    /// A no-op (`self == to`) is always allowed so other fields can be updated without a
    /// status change.
    #[must_use]
    pub fn can_transition_to(self, to: TicketStatus) -> bool {
        self == to || self.allowed_transitions().contains(&to)
    }
}

/// Resolution types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum Resolution {
    None = 0,
    Resolved = 1,
    Workaround = 2,
    CannotReproduce = 3,
    UnsupportedIssue = 4,
    Duplicate = 5,
    ServiceOutage = 6,
    UserError = 7,
}

impl Resolution {
    #[must_use]
    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => Self::Resolved,
            2 => Self::Workaround,
            3 => Self::CannotReproduce,
            4 => Self::UnsupportedIssue,
            5 => Self::Duplicate,
            6 => Self::ServiceOutage,
            7 => Self::UserError,
            _ => Self::None,
        }
    }
}

/// Next action scheduling
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NextAction {
    None,
    FollowUp(DateTime<Utc>),
    Appointment(DateTime<Utc>),
    AutoClose(AutoCloseSchedule),
}

/// Auto-close timeframe options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum AutoCloseSchedule {
    EndOfDay = 0,
    Hours24 = 24,
    Hours48 = 48,
    Hours72 = 72,
}

impl AutoCloseSchedule {
    /// Calculate the auto-close timestamp from now
    ///
    /// # Panics
    ///
    /// Panics if the end-of-day time calculation fails (should never happen with valid dates).
    #[must_use]
    pub fn calculate_close_time(self) -> DateTime<Utc> {
        let now = Utc::now();
        match self {
            Self::EndOfDay => {
                let today = now.date_naive();
                let eod = today
                    .and_hms_opt(23, 59, 59)
                    .expect("valid end of day time");
                DateTime::from_naive_utc_and_offset(eod, Utc)
            }
            Self::Hours24 => now + chrono::Duration::hours(24),
            Self::Hours48 => now + chrono::Duration::hours(48),
            Self::Hours72 => now + chrono::Duration::hours(72),
        }
    }
}

/// History entry for audit trail
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub timestamp: DateTime<Utc>,
    pub user_id: Uuid,
    pub field_changed: String,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
}

/// Main ticket structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ticket {
    /// Unique auto-incremented ticket number
    pub ticket_id: TicketId,

    /// Optional customer's own ticket reference
    pub customer_ticket_number: Option<String>,

    /// Optional ISP ticket reference
    pub isp_ticket_number: Option<String>,

    /// Optional partner ticket reference
    pub other_ticket_number: Option<String>,

    /// Brief summary of the issue
    pub title: String,

    /// Associated project/customer organization
    pub project: String,

    /// Account UUID
    pub account_uuid: Uuid,

    /// Primary symptom category
    pub symptom: Symptom,

    /// Ticket workflow priority
    pub priority: TicketPriority,

    /// Current workflow status
    pub status: TicketStatus,

    /// Scheduled next action
    pub next_action: NextAction,

    /// Resolution (if closed)
    pub resolution: Option<Resolution>,

    /// User currently editing the ticket
    pub locked_by: Option<Uuid>,

    /// Assigned user or team
    pub assigned_to: Option<Uuid>,

    /// Creator information
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,

    /// Last update information
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,

    /// Audit trail
    pub history: Vec<HistoryEntry>,

    /// Optional ebonding data
    pub ebond: Option<String>,

    /// Tracking URL for the company's Broadband Provisioning portal
    pub tracking_url: Option<String>,

    /// Network devices supported at this site
    pub network_devices: Vec<NetworkDevice>,

    /// Schema version for evolution support
    pub schema_version: u32,

    /// Custom fields (dynamic schema)
    pub custom_fields: std::collections::HashMap<String, String>,
}

impl Ticket {
    /// Create a new ticket with required fields
    #[must_use]
    pub fn new(
        ticket_id: TicketId,
        title: String,
        project: String,
        account_uuid: Uuid,
        symptom: Symptom,
        created_by: Uuid,
    ) -> Self {
        let now = Utc::now();
        Self {
            ticket_id,
            customer_ticket_number: None,
            isp_ticket_number: None,
            other_ticket_number: None,
            title,
            project,
            account_uuid,
            symptom,
            priority: TicketPriority::Unknown,
            status: TicketStatus::Open,
            next_action: NextAction::None,
            resolution: None,
            locked_by: None,
            assigned_to: None,
            created_by,
            created_at: now,
            updated_by: created_by,
            updated_at: now,
            history: Vec::new(),
            ebond: None,
            tracking_url: None,
            network_devices: Vec::new(),
            schema_version: 1,
            custom_fields: std::collections::HashMap::new(),
        }
    }

    /// Format ticket update for posting to the company's Broadband Provisioning portal
    #[must_use]
    pub fn format_tracking_update(
        &self,
        customer_name: &str,
        site_id: &str,
        address: &str,
        tech_notes: &str,
    ) -> String {
        let infovulcan_ticket = self.ticket_id;
        let customer_ticket = self.customer_ticket_number.as_deref().unwrap_or("N/A");
        let third_party = self.other_ticket_number.as_deref().unwrap_or("N/A");

        // Format network devices (modems/ONTs)
        let devices: Vec<String> = self
            .network_devices
            .iter()
            .filter(|d| {
                matches!(
                    d,
                    NetworkDevice::DslModem { .. }
                        | NetworkDevice::CoaxModem { .. }
                        | NetworkDevice::Ont { .. }
                )
            })
            .map(|d| {
                let mac = d
                    .mac_address()
                    .map_or("N/A".to_string(), std::string::ToString::to_string);
                format!("{}\n{}", d.make_model(), mac)
            })
            .collect();

        let device_info = if devices.is_empty() {
            "N/A\nN/A".to_string()
        } else {
            devices.join("\n\n")
        };

        format!(
            "INFOVULCAN: {infovulcan_ticket}\nCUSTOMER: {customer_ticket}\n3RD_PARTY: {third_party}\n\n{device_info}\n\n{customer_name}\n{site_id}\n{address}\n\n***\n\n{tech_notes}"
        )
    }

    /// Validate ticket state consistency
    ///
    /// # Errors
    ///
    /// Returns an error if the ticket state is inconsistent (e.g., closed without resolution)
    pub fn validate(&self) -> Result<(), crate::error::InfoVulcanError> {
        // Check status-specific requirements
        if self.status.requires_resolution() && self.resolution.is_none() {
            return Err(crate::error::InfoVulcanError::ValidationError(format!(
                "Status {:?} requires a resolution",
                self.status
            )));
        }

        if self.status.requires_next_action() && matches!(self.next_action, NextAction::None) {
            return Err(crate::error::InfoVulcanError::ValidationError(format!(
                "Status {:?} requires a next action",
                self.status
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_can_transition_to_working_and_terminal_states() {
        let open = TicketStatus::Open;
        assert!(open.can_transition_to(TicketStatus::AwaitingCustomer));
        assert!(open.can_transition_to(TicketStatus::Closed));
        assert!(open.can_transition_to(TicketStatus::AutoClose));
    }

    #[test]
    fn terminal_states_are_sinks() {
        for terminal in [TicketStatus::Closed, TicketStatus::AutoClose] {
            assert!(terminal.is_terminal());
            assert!(terminal.allowed_transitions().is_empty());
            // No outgoing transition except the no-op self-transition.
            assert!(terminal.can_transition_to(terminal));
            assert!(!terminal.can_transition_to(TicketStatus::Open));
            assert!(!terminal.can_transition_to(TicketStatus::AwaitingISP));
        }
    }

    #[test]
    fn noop_transition_is_always_allowed() {
        // Even a terminal state allows updating other fields without a status change.
        assert!(TicketStatus::Closed.can_transition_to(TicketStatus::Closed));
        assert!(TicketStatus::AwaitingISP.can_transition_to(TicketStatus::AwaitingISP));
    }

    #[test]
    fn disallowed_transition_is_rejected() {
        // EbondReceived's policy does not permit jumping straight to AppointmentScheduled.
        assert!(!TicketStatus::EbondReceived.can_transition_to(TicketStatus::AppointmentScheduled));
        // But it can go back to Open or to AwaitingISP.
        assert!(TicketStatus::EbondReceived.can_transition_to(TicketStatus::Open));
        assert!(TicketStatus::EbondReceived.can_transition_to(TicketStatus::AwaitingISP));
    }

    #[test]
    fn test_new_ticket_has_open_status() {
        let ticket = Ticket::new(
            1,
            "Test Issue".to_string(),
            "TestProject".to_string(),
            Uuid::new_v4(),
            Symptom::BroadbandDown,
            Uuid::new_v4(),
        );
        assert_eq!(ticket.status, TicketStatus::Open);
    }

    #[test]
    fn test_closed_ticket_requires_resolution() {
        let mut ticket = Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::SoftwareBug,
            Uuid::new_v4(),
        );
        ticket.status = TicketStatus::Closed;

        // Should fail validation without resolution
        assert!(ticket.validate().is_err());

        // Should pass with resolution
        ticket.resolution = Some(Resolution::Resolved);
        assert!(ticket.validate().is_ok());
    }

    #[test]
    fn test_open_ticket_requires_next_action() {
        let mut ticket = Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::HardwareFailure,
            Uuid::new_v4(),
        );
        ticket.next_action = NextAction::None;

        // Should fail validation for Open status without next action
        assert!(ticket.validate().is_err());

        // Should pass with next action
        ticket.next_action = NextAction::FollowUp(Utc::now());
        assert!(ticket.validate().is_ok());
    }

    #[test]
    fn test_auto_close_schedule_calculations() {
        let schedule = AutoCloseSchedule::Hours24;
        let close_time = schedule.calculate_close_time();
        let now = Utc::now();

        // Should be approximately 24 hours from now (within 1 minute tolerance)
        let diff = (close_time - now).num_minutes();
        assert!((1439..=1441).contains(&diff)); // 24 hours = 1440 minutes
    }

    #[test]
    fn test_mac_address_validation() {
        // Valid MAC addresses
        assert!(MacAddress::new("00:1A:2B:3C:4D:5E").is_ok());
        assert!(MacAddress::new("00-1A-2B-3C-4D-5E").is_ok());
        assert!(MacAddress::new("aa:bb:cc:dd:ee:ff").is_ok());

        // Invalid MAC addresses
        assert!(MacAddress::new("invalid").is_err());
        assert!(MacAddress::new("00:1A:2B:3C:4D").is_err()); // Too short
        assert!(MacAddress::new("00:1A:2B:3C:4D:5E:6F").is_err()); // Too long
        assert!(MacAddress::new("ZZ:1A:2B:3C:4D:5E").is_err()); // Invalid hex
    }

    #[test]
    fn test_mac_address_normalization() {
        let mac1 = MacAddress::new("aa-bb-cc-dd-ee-ff").unwrap();
        let mac2 = MacAddress::new("AA:BB:CC:DD:EE:FF").unwrap();

        // Should normalize to uppercase with colons
        assert_eq!(mac1.as_str(), "AA:BB:CC:DD:EE:FF");
        assert_eq!(mac2.as_str(), "AA:BB:CC:DD:EE:FF");
        assert_eq!(mac1, mac2);
    }

    #[test]
    fn test_network_device_creation() {
        let mac = MacAddress::new("00:11:22:33:44:55").unwrap();
        let device = NetworkDevice::CoaxModem {
            make: "Motorola".to_string(),
            model: "MB8600".to_string(),
            mac_address: Some(mac),
            serial_number: Some("SN123456".to_string()),
        };

        assert_eq!(device.device_type(), "Coax Modem");
        assert_eq!(device.make_model(), "Motorola MB8600");
        assert!(device.mac_address().is_some());
    }

    #[test]
    fn test_tracking_update_format() {
        let mut ticket = Ticket::new(
            12345,
            "Internet Down".to_string(),
            "ACME Corp".to_string(),
            Uuid::new_v4(),
            Symptom::BroadbandDown,
            Uuid::new_v4(),
        );

        ticket.customer_ticket_number = Some("CUST-999".to_string());
        ticket.other_ticket_number = Some("ISP-777".to_string());

        // Add a modem
        let mac = MacAddress::new("AA:BB:CC:DD:EE:FF").unwrap();
        ticket.network_devices.push(NetworkDevice::CoaxModem {
            make: "Arris".to_string(),
            model: "SB8200".to_string(),
            mac_address: Some(mac),
            serial_number: None,
        });

        let update = ticket.format_tracking_update(
            "John Doe",
            "SITE-001",
            "123 Main St, Anytown, USA",
            "Replaced faulty modem. Service restored.",
        );

        // Verify format
        assert!(update.contains("INFOVULCAN: 12345"));
        assert!(update.contains("CUSTOMER: CUST-999"));
        assert!(update.contains("3RD_PARTY: ISP-777"));
        assert!(update.contains("Arris SB8200"));
        assert!(update.contains("AA:BB:CC:DD:EE:FF"));
        assert!(update.contains("John Doe"));
        assert!(update.contains("SITE-001"));
        assert!(update.contains("123 Main St, Anytown, USA"));
        assert!(update.contains("***"));
        assert!(update.contains("Replaced faulty modem. Service restored."));
    }

    #[test]
    fn test_tracking_update_without_optional_fields() {
        let ticket = Ticket::new(
            999,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::ConfigurationError,
            Uuid::new_v4(),
        );

        let update = ticket.format_tracking_update(
            "Jane Smith",
            "SITE-002",
            "456 Oak Ave",
            "No issues found.",
        );

        // Should handle missing optional fields gracefully
        assert!(update.contains("INFOVULCAN: 999"));
        assert!(update.contains("CUSTOMER: N/A"));
        assert!(update.contains("3RD_PARTY: N/A"));
        assert!(update.contains("N/A\nN/A")); // No devices
    }

    #[test]
    fn test_multiple_network_devices() {
        let mut ticket = Ticket::new(
            555,
            "Equipment Upgrade".to_string(),
            "Test Project".to_string(),
            Uuid::new_v4(),
            Symptom::HardwareFailure,
            Uuid::new_v4(),
        );

        // Add multiple devices
        ticket.network_devices.push(NetworkDevice::Ont {
            make: "Nokia".to_string(),
            model: "G-010S-A".to_string(),
            mac_address: Some(MacAddress::new("11:22:33:44:55:66").unwrap()),
            serial_number: Some("ONT123".to_string()),
        });

        ticket.network_devices.push(NetworkDevice::Router {
            make: "Ubiquiti".to_string(),
            model: "EdgeRouter X".to_string(),
            mac_address: Some(MacAddress::new("AA:BB:CC:DD:EE:FF").unwrap()),
            serial_number: None,
        });

        ticket.network_devices.push(NetworkDevice::Firewall {
            make: "pfSense".to_string(),
            model: "SG-3100".to_string(),
            mac_address: None,
            serial_number: Some("FW999".to_string()),
        });

        assert_eq!(ticket.network_devices.len(), 3);

        // Tracking update should only include modem/ONT devices
        let update = ticket.format_tracking_update("Test User", "SITE-X", "Address", "Notes");
        assert!(update.contains("Nokia G-010S-A"));
        assert!(update.contains("11:22:33:44:55:66"));
        // Router and Firewall should not appear in tracking update
        assert!(!update.contains("Ubiquiti"));
        assert!(!update.contains("pfSense"));
    }

    #[test]
    fn test_mac_address_display() {
        let mac = MacAddress::new("aa:bb:cc:dd:ee:ff").unwrap();
        assert_eq!(format!("{mac}"), "AA:BB:CC:DD:EE:FF");
    }

    #[test]
    fn test_all_network_device_types() {
        let devices = [
            NetworkDevice::DslModem {
                make: "ZyXEL".to_string(),
                model: "C3000Z".to_string(),
                mac_address: None,
                serial_number: None,
            },
            NetworkDevice::FixedWirelessAntenna {
                make: "Ubiquiti".to_string(),
                model: "LiteBeam".to_string(),
                mac_address: None,
                serial_number: None,
            },
            NetworkDevice::VpnGw {
                make: "Cisco".to_string(),
                model: "ASA5505".to_string(),
                mac_address: None,
                serial_number: None,
            },
            NetworkDevice::Switch {
                make: "Netgear".to_string(),
                model: "GS108".to_string(),
                mac_address: None,
                serial_number: None,
            },
            NetworkDevice::Router {
                make: "MikroTik".to_string(),
                model: "hEX".to_string(),
                mac_address: None,
                serial_number: None,
            },
        ];

        assert_eq!(devices[0].device_type(), "DSL Modem");
        assert_eq!(devices[1].device_type(), "Fixed Wireless Antenna");
        assert_eq!(devices[2].device_type(), "VPN Gateway");
        assert_eq!(devices[3].device_type(), "Switch");
        assert_eq!(devices[4].device_type(), "Router");

        assert_eq!(devices[0].make_model(), "ZyXEL C3000Z");
        assert_eq!(devices[1].make_model(), "Ubiquiti LiteBeam");
        assert_eq!(devices[2].make_model(), "Cisco ASA5505");
        assert_eq!(devices[3].make_model(), "Netgear GS108");
        assert_eq!(devices[4].make_model(), "MikroTik hEX");
    }

    #[test]
    fn test_auto_close_schedule_all_timeframes() {
        // Test all timeframes calculate valid times
        let schedules = [
            AutoCloseSchedule::EndOfDay,
            AutoCloseSchedule::Hours48,
            AutoCloseSchedule::Hours72,
        ];

        let now = Utc::now();
        for schedule in schedules {
            let close_time = schedule.calculate_close_time();
            assert!(close_time > now); // Should be in the future
        }
    }

    #[test]
    fn test_ticket_validation_with_support_hold_status() {
        let mut ticket = Ticket::new(
            100,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::PacketLoss,
            Uuid::new_v4(),
        );

        // SupportHold status doesn't require resolution or next_action
        ticket.status = TicketStatus::SupportHold;
        ticket.next_action = NextAction::None;
        ticket.resolution = None;

        assert!(ticket.validate().is_ok());
    }

    #[test]
    fn test_ticket_with_multiple_optional_fields() {
        let mut ticket = Ticket::new(
            200,
            "Complex Ticket".to_string(),
            "Enterprise".to_string(),
            Uuid::new_v4(),
            Symptom::VpnIssue,
            Uuid::new_v4(),
        );

        ticket.customer_ticket_number = Some("CUST123".to_string());
        ticket.isp_ticket_number = Some("ISP456".to_string());
        ticket.other_ticket_number = Some("PART789".to_string());
        ticket.tracking_url = Some("https://portal.example.com/track/200".to_string());
        ticket.ebond = Some("EBOND-DATA".to_string());

        assert!(ticket.customer_ticket_number.is_some());
        assert!(ticket.tracking_url.is_some());
        assert_eq!(ticket.ebond.as_deref(), Some("EBOND-DATA"));
    }

    #[test]
    fn test_history_entry_creation() {
        let entry = HistoryEntry {
            timestamp: Utc::now(),
            user_id: Uuid::new_v4(),
            field_changed: "status".to_string(),
            old_value: Some("Open".to_string()),
            new_value: Some("Closed".to_string()),
        };

        assert_eq!(entry.field_changed, "status");
        assert!(entry.old_value.is_some());
        assert!(entry.new_value.is_some());
    }

    #[test]
    fn test_ticket_status_requirements() {
        assert!(TicketStatus::Closed.requires_resolution());
        assert!(TicketStatus::AutoClose.requires_resolution());
        assert!(!TicketStatus::Open.requires_resolution());

        assert!(TicketStatus::Open.requires_next_action());
        assert!(TicketStatus::AwaitingCustomer.requires_next_action());
        assert!(TicketStatus::AwaitingISP.requires_next_action());
        assert!(TicketStatus::AwaitingPartner.requires_next_action());
        assert!(!TicketStatus::Closed.requires_next_action());
    }

    #[test]
    fn test_all_symptom_variants() {
        let symptoms = [
            Symptom::Unknown,
            Symptom::BroadbandDown,
            Symptom::BroadbandIntermittent,
            Symptom::PacketLoss,
            Symptom::Power,
            Symptom::VpnIssue,
            Symptom::ConfigurationError,
            Symptom::HardwareFailure,
            Symptom::SoftwareBug,
            Symptom::SecurityIncident,
            Symptom::SlowBandwidth,
            Symptom::DuplexingMismatch,
            Symptom::LatencyIssues,
            Symptom::JitterProblems,
            Symptom::DnsIssues,
            Symptom::Other,
        ];

        // Ensure all variants are distinct
        for (i, symptom1) in symptoms.iter().enumerate() {
            for (j, symptom2) in symptoms.iter().enumerate() {
                if i != j {
                    assert_ne!(symptom1, symptom2);
                }
            }
        }
    }

    #[test]
    fn test_ticket_priority_enum() {
        assert_eq!(TicketPriority::from_u8(0), TicketPriority::Unknown);
        assert_eq!(TicketPriority::from_u8(1), TicketPriority::HardDown);
        assert_eq!(TicketPriority::from_u8(2), TicketPriority::PrimaryDown);
        assert_eq!(TicketPriority::from_u8(3), TicketPriority::BackupDown);
        assert_eq!(TicketPriority::from_u8(4), TicketPriority::Intermittent);
        assert_eq!(TicketPriority::from_u8(5), TicketPriority::PacketLoss);
        assert_eq!(TicketPriority::from_u8(255), TicketPriority::Unknown); // Invalid value defaults to Unknown
    }

    #[test]
    fn test_ticket_with_priority() {
        let mut ticket = Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::BroadbandDown,
            Uuid::new_v4(),
        );

        // Initially Unknown priority
        assert_eq!(ticket.priority, TicketPriority::Unknown);

        // Set priority
        ticket.priority = TicketPriority::HardDown;
        assert_eq!(ticket.priority, TicketPriority::HardDown);
    }

    // ── from_u8 helpers ───────────────────────────────────────────────────────

    #[test]
    fn test_symptom_from_u8_all_variants() {
        assert_eq!(Symptom::from_u8(0), Symptom::Unknown);
        assert_eq!(Symptom::from_u8(1), Symptom::BroadbandDown);
        assert_eq!(Symptom::from_u8(2), Symptom::BroadbandIntermittent);
        assert_eq!(Symptom::from_u8(3), Symptom::PacketLoss);
        assert_eq!(Symptom::from_u8(4), Symptom::Power);
        assert_eq!(Symptom::from_u8(5), Symptom::VpnIssue);
        assert_eq!(Symptom::from_u8(6), Symptom::ConfigurationError);
        assert_eq!(Symptom::from_u8(7), Symptom::HardwareFailure);
        assert_eq!(Symptom::from_u8(8), Symptom::SoftwareBug);
        assert_eq!(Symptom::from_u8(9), Symptom::SecurityIncident);
        assert_eq!(Symptom::from_u8(10), Symptom::SlowBandwidth);
        assert_eq!(Symptom::from_u8(11), Symptom::DuplexingMismatch);
        assert_eq!(Symptom::from_u8(12), Symptom::LatencyIssues);
        assert_eq!(Symptom::from_u8(13), Symptom::JitterProblems);
        assert_eq!(Symptom::from_u8(14), Symptom::DnsIssues);
        assert_eq!(Symptom::from_u8(255), Symptom::Other);
        assert_eq!(Symptom::from_u8(100), Symptom::Unknown); // unrecognised → Unknown
    }

    #[test]
    fn test_ticket_status_from_u8_all_variants() {
        assert_eq!(TicketStatus::from_u8(0), TicketStatus::Open);
        assert_eq!(TicketStatus::from_u8(1), TicketStatus::AwaitingCustomer);
        assert_eq!(TicketStatus::from_u8(2), TicketStatus::AwaitingISP);
        assert_eq!(TicketStatus::from_u8(3), TicketStatus::AwaitingPartner);
        assert_eq!(TicketStatus::from_u8(4), TicketStatus::SupportHold);
        assert_eq!(TicketStatus::from_u8(5), TicketStatus::HandedOff);
        assert_eq!(TicketStatus::from_u8(6), TicketStatus::AppointmentScheduled);
        assert_eq!(TicketStatus::from_u8(7), TicketStatus::EbondReceived);
        assert_eq!(TicketStatus::from_u8(8), TicketStatus::VoicemailReceived);
        assert_eq!(TicketStatus::from_u8(254), TicketStatus::AutoClose);
        assert_eq!(TicketStatus::from_u8(255), TicketStatus::Closed);
        assert_eq!(TicketStatus::from_u8(100), TicketStatus::Open); // unrecognised → Open
    }

    #[test]
    fn test_resolution_from_u8_all_variants() {
        assert_eq!(Resolution::from_u8(0), Resolution::None);
        assert_eq!(Resolution::from_u8(1), Resolution::Resolved);
        assert_eq!(Resolution::from_u8(2), Resolution::Workaround);
        assert_eq!(Resolution::from_u8(3), Resolution::CannotReproduce);
        assert_eq!(Resolution::from_u8(4), Resolution::UnsupportedIssue);
        assert_eq!(Resolution::from_u8(5), Resolution::Duplicate);
        assert_eq!(Resolution::from_u8(6), Resolution::ServiceOutage);
        assert_eq!(Resolution::from_u8(7), Resolution::UserError);
        assert_eq!(Resolution::from_u8(200), Resolution::None); // unrecognised → None
    }

    #[test]
    fn test_ont_and_firewall_device_type_and_make_model() {
        let ont = NetworkDevice::Ont {
            make: "Nokia".to_string(),
            model: "G-010S-A".to_string(),
            mac_address: None,
            serial_number: None,
        };
        assert_eq!(ont.device_type(), "ONT");
        assert_eq!(ont.make_model(), "Nokia G-010S-A");
        assert!(ont.mac_address().is_none());

        let firewall = NetworkDevice::Firewall {
            make: "pfSense".to_string(),
            model: "SG-3100".to_string(),
            mac_address: Some(MacAddress::new("AA:BB:CC:DD:EE:FF").unwrap()),
            serial_number: None,
        };
        assert_eq!(firewall.device_type(), "Firewall");
        assert_eq!(firewall.make_model(), "pfSense SG-3100");
        assert!(firewall.mac_address().is_some());
    }

    #[test]
    fn test_tracking_update_shows_na_for_device_without_mac() {
        // Ensures the mac_address().map_or("N/A", ...) branch is exercised.
        let mut ticket = Ticket::new(
            1,
            "Test".to_string(),
            "Project".to_string(),
            Uuid::new_v4(),
            Symptom::BroadbandDown,
            Uuid::new_v4(),
        );
        ticket.network_devices.push(NetworkDevice::DslModem {
            make: "ASUS".to_string(),
            model: "DSL-N14U".to_string(),
            mac_address: None, // no MAC → should render as "N/A"
            serial_number: None,
        });
        let update = ticket.format_tracking_update("Tech", "SITE-1", "123 Main St", "Notes");
        assert!(update.contains("ASUS DSL-N14U"));
        assert!(update.contains("N/A"));
    }

    #[test]
    fn mac_address_accessor_returns_mac_for_remaining_variants() {
        // device_type/make_model are exercised elsewhere; this covers mac_address() for the
        // wireless / gateway / switch / router arms specifically.
        let mac = MacAddress::new("00:11:22:33:44:55").unwrap();
        let devices = [
            NetworkDevice::FixedWirelessAntenna {
                make: "Ubiquiti".to_string(),
                model: "LiteBeam".to_string(),
                mac_address: Some(mac.clone()),
                serial_number: None,
            },
            NetworkDevice::VpnGw {
                make: "Cisco".to_string(),
                model: "ASA".to_string(),
                mac_address: Some(mac.clone()),
                serial_number: None,
            },
            NetworkDevice::Switch {
                make: "Netgear".to_string(),
                model: "GS108".to_string(),
                mac_address: Some(mac.clone()),
                serial_number: None,
            },
            NetworkDevice::Router {
                make: "MikroTik".to_string(),
                model: "hEX".to_string(),
                mac_address: Some(mac.clone()),
                serial_number: None,
            },
        ];
        for d in &devices {
            assert_eq!(d.mac_address(), Some(&mac));
        }
    }

    #[test]
    fn allowed_transitions_enumerated_for_every_non_terminal_state() {
        use TicketStatus::{
            AppointmentScheduled, AutoClose, AwaitingCustomer, AwaitingISP, AwaitingPartner,
            Closed, EbondReceived, HandedOff, Open, SupportHold, VoicemailReceived,
        };
        // Each non-terminal source state exposes a non-empty allow-list and can always reach
        // both terminal sinks plus return to Open.
        let sources = [
            AwaitingCustomer,
            AwaitingISP,
            AwaitingPartner,
            SupportHold,
            HandedOff,
            AppointmentScheduled,
            EbondReceived,
            VoicemailReceived,
        ];
        for s in sources {
            assert!(!s.allowed_transitions().is_empty());
            assert!(s.can_transition_to(Closed));
            assert!(s.can_transition_to(AutoClose));
            assert!(s.can_transition_to(Open));
        }
        // Representative legal / illegal edges unique to specific states.
        assert!(AwaitingPartner.can_transition_to(HandedOff));
        assert!(!AwaitingISP.can_transition_to(HandedOff));
        assert!(SupportHold.can_transition_to(AppointmentScheduled));
        assert!(AppointmentScheduled.can_transition_to(AwaitingCustomer));
        assert!(EbondReceived.can_transition_to(AwaitingISP));
        assert!(VoicemailReceived.can_transition_to(AwaitingCustomer));
    }
}
