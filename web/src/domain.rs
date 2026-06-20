//! Front-end mirror of the ticket status/priority vocabulary and the **policy-as-code**
//! state-transition matrix that lives authoritatively in `shared::ticket::TicketStatus`.
//!
//! The custodian service enforces transitions for real; this mirror drives the UI so a user is
//! only offered valid next statuses (and is warned before submitting an illegal one). The numeric
//! values and the allow-lists below are kept deliberately identical to `shared/src/ticket.rs` —
//! if you change the workflow there, change it here too. The web crate intentionally does not
//! depend on `shared` to avoid pulling the server-side crypto stack into the WASM bundle.

/// Status discriminants (mirror of `shared::TicketStatus as u8`).
pub const OPEN: i32 = 0;
pub const AWAITING_CUSTOMER: i32 = 1;
pub const AWAITING_ISP: i32 = 2;
pub const AWAITING_PARTNER: i32 = 3;
pub const SUPPORT_HOLD: i32 = 4;
pub const HANDED_OFF: i32 = 5;
pub const APPOINTMENT_SCHEDULED: i32 = 6;
pub const EBOND_RECEIVED: i32 = 7;
pub const VOICEMAIL_RECEIVED: i32 = 8;
pub const AUTO_CLOSE: i32 = 254;
pub const CLOSED: i32 = 255;

/// All known statuses as `(value, label)`, in workflow order. Drives dropdowns.
pub const STATUSES: &[(i32, &str)] = &[
    (OPEN, "Open"),
    (AWAITING_CUSTOMER, "Awaiting Customer"),
    (AWAITING_ISP, "Awaiting ISP"),
    (AWAITING_PARTNER, "Awaiting Partner"),
    (SUPPORT_HOLD, "Support Hold"),
    (HANDED_OFF, "Handed Off"),
    (APPOINTMENT_SCHEDULED, "Appointment Scheduled"),
    (EBOND_RECEIVED, "Ebond Received"),
    (VOICEMAIL_RECEIVED, "Voicemail Received"),
    (AUTO_CLOSE, "Auto-Close"),
    (CLOSED, "Closed"),
];

/// All known priorities as `(value, label)` (mirror of `shared::TicketPriority`).
pub const PRIORITIES: &[(i32, &str)] = &[
    (0, "Unknown"),
    (1, "Hard Down"),
    (2, "Primary Down"),
    (3, "Backup Down"),
    (4, "Intermittent"),
    (5, "Packet Loss"),
];

/// All user roles as `(value, label)` (mirror of `admin` proto `Role` / `shared::user::Role`).
pub const ROLES: &[(i32, &str)] = &[
    (0, "Admin"),
    (1, "Manager"),
    (2, "Supervisor"),
    (3, "Technician"),
    (4, "Ebond Partner"),
    (5, "Read Only"),
];

/// Human-readable label for a status value (falls back to the raw number).
#[must_use]
pub fn status_label(value: i32) -> String {
    STATUSES.iter().find(|(v, _)| *v == value).map_or_else(
        || format!("Status {value}"),
        |(_, label)| (*label).to_string(),
    )
}

/// Human-readable label for a priority value (falls back to the raw number).
#[must_use]
pub fn priority_label(value: i32) -> String {
    PRIORITIES.iter().find(|(v, _)| *v == value).map_or_else(
        || format!("Priority {value}"),
        |(_, label)| (*label).to_string(),
    )
}

/// Whether `value` is a terminal status (`Closed` / `AutoClose`) — a lifecycle sink.
#[must_use]
pub fn is_terminal(value: i32) -> bool {
    value == CLOSED || value == AUTO_CLOSE
}

/// The statuses a ticket may move **to** from `from` (mirror of
/// `shared::TicketStatus::allowed_transitions`). Terminal/unknown states return an empty list.
#[must_use]
pub fn allowed_transitions(from: i32) -> &'static [i32] {
    match from {
        OPEN => &[
            AWAITING_CUSTOMER,
            AWAITING_ISP,
            AWAITING_PARTNER,
            SUPPORT_HOLD,
            HANDED_OFF,
            APPOINTMENT_SCHEDULED,
            EBOND_RECEIVED,
            VOICEMAIL_RECEIVED,
            CLOSED,
            AUTO_CLOSE,
        ],
        AWAITING_CUSTOMER => &[
            OPEN,
            SUPPORT_HOLD,
            APPOINTMENT_SCHEDULED,
            EBOND_RECEIVED,
            VOICEMAIL_RECEIVED,
            CLOSED,
            AUTO_CLOSE,
        ],
        AWAITING_ISP => &[OPEN, SUPPORT_HOLD, EBOND_RECEIVED, CLOSED, AUTO_CLOSE],
        AWAITING_PARTNER => &[OPEN, SUPPORT_HOLD, HANDED_OFF, CLOSED, AUTO_CLOSE],
        SUPPORT_HOLD => &[
            OPEN,
            AWAITING_CUSTOMER,
            AWAITING_ISP,
            AWAITING_PARTNER,
            APPOINTMENT_SCHEDULED,
            CLOSED,
            AUTO_CLOSE,
        ],
        HANDED_OFF => &[OPEN, AWAITING_PARTNER, CLOSED, AUTO_CLOSE],
        APPOINTMENT_SCHEDULED => &[OPEN, AWAITING_CUSTOMER, SUPPORT_HOLD, CLOSED, AUTO_CLOSE],
        EBOND_RECEIVED => &[OPEN, AWAITING_ISP, CLOSED, AUTO_CLOSE],
        VOICEMAIL_RECEIVED => &[OPEN, AWAITING_CUSTOMER, CLOSED, AUTO_CLOSE],
        _ => &[],
    }
}

/// Policy check mirroring `shared::TicketStatus::can_transition_to`: a no-op (`from == to`) is
/// always allowed; otherwise `to` must be in the allow-list for `from`.
#[must_use]
pub fn can_transition(from: i32, to: i32) -> bool {
    from == to || allowed_transitions(from).contains(&to)
}

/// Count occurrences of each distinct value, returned sorted ascending by value. Drives the
/// "by status" / "by priority" analytics breakdowns.
#[must_use]
pub fn tally(values: &[i32]) -> Vec<(i32, usize)> {
    let mut counts: std::collections::BTreeMap<i32, usize> = std::collections::BTreeMap::new();
    for &v in values {
        *counts.entry(v).or_default() += 1;
    }
    counts.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_can_reach_working_and_terminal_states() {
        assert!(can_transition(OPEN, AWAITING_CUSTOMER));
        assert!(can_transition(OPEN, CLOSED));
        assert!(can_transition(OPEN, AUTO_CLOSE));
    }

    #[test]
    fn terminal_states_are_sinks() {
        for terminal in [CLOSED, AUTO_CLOSE] {
            assert!(is_terminal(terminal));
            assert!(allowed_transitions(terminal).is_empty());
            // self-transition always allowed
            assert!(can_transition(terminal, terminal));
            assert!(!can_transition(terminal, OPEN));
        }
    }

    #[test]
    fn illegal_transition_is_rejected() {
        // Ebond cannot jump straight to an appointment (mirror of the shared test).
        assert!(!can_transition(EBOND_RECEIVED, APPOINTMENT_SCHEDULED));
        assert!(can_transition(EBOND_RECEIVED, OPEN));
    }

    #[test]
    fn tally_counts_and_sorts() {
        let counts = tally(&[2, 0, 2, 1, 2, 0]);
        assert_eq!(counts, vec![(0, 2), (1, 1), (2, 3)]);
        assert!(tally(&[]).is_empty());
    }

    #[test]
    fn labels_have_fallbacks() {
        assert_eq!(status_label(OPEN), "Open");
        assert_eq!(status_label(9999), "Status 9999");
        assert_eq!(priority_label(1), "Hard Down");
        assert_eq!(priority_label(9999), "Priority 9999");
    }
}
