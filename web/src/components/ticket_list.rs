use crate::api::{
    self, CreateTicketRequest, CreateUserRequest, ListTicketsFilter, NextActionInput, Ticket,
    UpdateTicketRequest,
};
use crate::domain;
use crate::theme;
use leptos::*;
use leptos_router::use_navigate;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn TicketList() -> impl IntoView {
    let token = api::get_token();
    let navigate = use_navigate();

    // ── Theme ────────────────────────────────────────────────────────────────
    let (theme_sig, set_theme_sig) = create_signal(theme::current_theme());

    // ── Search results / dashboard source data ────────────────────────────────
    let (tickets, set_tickets) = create_signal::<Vec<Ticket>>(Vec::new());
    let (message, set_message) = create_signal(String::new());
    let (error, set_error) = create_signal(String::new());

    // ── Filter inputs ─────────────────────────────────────────────────────────
    let (filter_status, set_filter_status) = create_signal("any".to_string());
    let (filter_assignee, set_filter_assignee) = create_signal(String::new());
    let (filter_account, set_filter_account) = create_signal(String::new());
    let (filter_project, set_filter_project) = create_signal(String::new());
    let (filter_include_deleted, set_filter_include_deleted) = create_signal(false);
    let (filter_limit, set_filter_limit) = create_signal(String::new());

    // ── Loaded ticket + update form ───────────────────────────────────────────
    let (ticket, set_ticket) = create_signal::<Option<Ticket>>(None);
    let (lookup_id, set_lookup_id) = create_signal(String::new());
    let (update_title, set_update_title) = create_signal(String::new());
    let (update_project, set_update_project) = create_signal(String::new());
    let (update_priority, set_update_priority) = create_signal(String::new());
    let (update_status, set_update_status) = create_signal(String::new());
    // Next-action editor: kind is one of "unchanged" | "none" | "follow_up" | "appointment"
    // | "auto_close". "unchanged" omits the field; the others set/clear it.
    let (update_na_kind, set_update_na_kind) = create_signal("unchanged".to_string());
    let (update_na_at, set_update_na_at) = create_signal(String::new());
    let (update_na_schedule, set_update_na_schedule) = create_signal("end_of_day".to_string());

    // ── Create ticket ─────────────────────────────────────────────────────────
    let (new_title, set_new_title) = create_signal(String::new());
    let (new_project, set_new_project) = create_signal(String::new());
    let (new_account_uuid, set_new_account_uuid) = create_signal(String::new());
    let (new_priority, set_new_priority) = create_signal("1".to_string());

    // ── Create user ───────────────────────────────────────────────────────────
    let (create_user_name, set_create_user_name) = create_signal(String::new());
    let (create_user_password, set_create_user_password) = create_signal(String::new());
    let (create_user_email, set_create_user_email) = create_signal(String::new());
    let (create_user_display_name, set_create_user_display_name) = create_signal(String::new());
    let (create_user_role, set_create_user_role) = create_signal("2".to_string());

    // ── Analytics (derived from the current result set) ───────────────────────
    let total = create_memo(move |_| tickets.get().len());
    let status_counts = create_memo(move |_| {
        let values: Vec<i32> = tickets.get().iter().map(|t| t.status).collect();
        domain::tally(&values)
    });
    let priority_counts = create_memo(move |_| {
        let values: Vec<i32> = tickets.get().iter().map(|t| t.priority).collect();
        domain::tally(&values)
    });

    // Status options for the update form: the current status plus its legal transitions
    // (policy-as-code mirror of the custodian's state machine). Falls back to all statuses.
    let update_status_options = create_memo(move |_| match ticket.get() {
        Some(t) => {
            let mut opts = vec![t.status];
            opts.extend_from_slice(domain::allowed_transitions(t.status));
            opts
        }
        None => domain::STATUSES.iter().map(|(v, _)| *v).collect(),
    });

    // Load a ticket (from a result row) into the update form.
    let load_into_form = move |t: Ticket| {
        set_update_title.set(t.title.clone());
        set_update_project.set(t.project.clone());
        set_update_priority.set(t.priority.to_string());
        set_update_status.set(t.status.to_string());
        // Reflect the ticket's current next action into the editor (defaults to "unchanged").
        match &t.next_action {
            Some(api::NextAction::FollowUp { at }) => {
                set_update_na_kind.set("follow_up".to_string());
                set_update_na_at.set(at.to_string());
            }
            Some(api::NextAction::Appointment { at }) => {
                set_update_na_kind.set("appointment".to_string());
                set_update_na_at.set(at.to_string());
            }
            Some(api::NextAction::AutoClose { schedule }) => {
                set_update_na_kind.set("auto_close".to_string());
                set_update_na_schedule.set(schedule.clone());
            }
            None => set_update_na_kind.set("unchanged".to_string()),
        }
        set_ticket.set(Some(t));
    };

    let on_toggle_theme = move |_| {
        set_theme_sig.set(theme::toggle());
    };

    let on_lookup = {
        let token = token.clone();
        move |_| {
            let Some(token) = token.clone() else {
                set_error.set("Missing auth token. Please sign in again.".to_string());
                return;
            };
            let Ok(ticket_id) = lookup_id.get().trim().parse::<u64>() else {
                set_error.set("Ticket number must be a valid integer.".to_string());
                return;
            };
            set_error.set(String::new());
            set_message.set(String::new());
            spawn_local(async move {
                match api::fetch_ticket(&token, ticket_id).await {
                    Ok(found) => load_into_form(found),
                    Err(err) => set_error.set(err.to_string()),
                }
            });
        }
    };

    let on_search = {
        let token = token.clone();
        move |_| {
            let Some(token) = token.clone() else {
                set_error.set("Missing auth token. Please sign in again.".to_string());
                return;
            };
            let status = match filter_status.get().as_str() {
                "any" => None,
                other => other.parse::<i32>().ok(),
            };
            let filter = ListTicketsFilter {
                status,
                assignee: filter_assignee.get(),
                account: filter_account.get(),
                project: filter_project.get(),
                include_deleted: filter_include_deleted.get(),
                limit: filter_limit.get().trim().parse::<u32>().ok(),
            };
            set_error.set(String::new());
            set_message.set(String::new());
            spawn_local(async move {
                match api::list_tickets(&token, &filter).await {
                    Ok(list) => {
                        set_message.set(format!("Found {} ticket(s).", list.len()));
                        set_tickets.set(list);
                    }
                    Err(err) => set_error.set(err.to_string()),
                }
            });
        }
    };

    let on_create_ticket = {
        let token = token.clone();
        move |_| {
            let Some(token) = token.clone() else {
                set_error.set("Missing auth token. Please sign in again.".to_string());
                return;
            };
            let Ok(priority) = new_priority.get().parse::<i32>() else {
                set_error.set("Priority must be a valid integer enum value.".to_string());
                return;
            };
            let payload = CreateTicketRequest {
                title: new_title.get(),
                project: new_project.get(),
                account_uuid: new_account_uuid.get(),
                symptom: 1,
                priority,
            };
            set_error.set(String::new());
            set_message.set(String::new());
            spawn_local(async move {
                match api::create_ticket(&token, &payload).await {
                    Ok(created) => {
                        set_message.set(format!("Ticket #{} created.", created.ticket_id));
                        set_update_title.set(created.title.clone());
                        set_update_project.set(created.project.clone());
                        set_update_priority.set(created.priority.to_string());
                        set_update_status.set(created.status.to_string());
                        set_ticket.set(Some(created));
                    }
                    Err(err) => set_error.set(err.to_string()),
                }
            });
        }
    };

    let on_update_ticket = {
        let token = token.clone();
        move |_| {
            let Some(token) = token.clone() else {
                set_error.set("Missing auth token. Please sign in again.".to_string());
                return;
            };
            let Some(current) = ticket.get() else {
                set_error.set("Load a ticket before updating.".to_string());
                return;
            };

            let priority = if update_priority.get().trim().is_empty() {
                None
            } else if let Ok(value) = update_priority.get().parse::<i32>() {
                Some(value)
            } else {
                set_error.set("Update priority must be a valid integer enum value.".to_string());
                return;
            };

            let status = if update_status.get().trim().is_empty() {
                None
            } else if let Ok(value) = update_status.get().parse::<i32>() {
                Some(value)
            } else {
                set_error.set("Update status must be a valid integer enum value.".to_string());
                return;
            };

            // Policy-as-code guard: reject an illegal status transition before hitting the API.
            if let Some(next) = status {
                if !domain::can_transition(current.status, next) {
                    set_error.set(format!(
                        "Illegal transition: {} → {} is not allowed.",
                        domain::status_label(current.status),
                        domain::status_label(next),
                    ));
                    return;
                }
            }

            // Build the optional next-action change. "unchanged" omits the field entirely;
            // the timestamp variants require a valid unix-seconds integer.
            let next_action = match update_na_kind.get().as_str() {
                "none" => Some(NextActionInput::None),
                "follow_up" | "appointment" => {
                    let Ok(at) = update_na_at.get().trim().parse::<i64>() else {
                        set_error
                            .set("Next-action time must be a unix-seconds integer.".to_string());
                        return;
                    };
                    if update_na_kind.get() == "follow_up" {
                        Some(NextActionInput::FollowUp { at })
                    } else {
                        Some(NextActionInput::Appointment { at })
                    }
                }
                "auto_close" => Some(NextActionInput::AutoClose {
                    schedule: update_na_schedule.get(),
                }),
                _ => None, // "unchanged"
            };

            let payload = UpdateTicketRequest {
                title: (!update_title.get().trim().is_empty()).then(|| update_title.get()),
                project: (!update_project.get().trim().is_empty()).then(|| update_project.get()),
                priority,
                status,
                next_action,
            };
            set_error.set(String::new());
            set_message.set(String::new());
            let ticket_id = current.ticket_id;
            spawn_local(async move {
                match api::update_ticket(&token, ticket_id, &payload).await {
                    Ok(updated) => {
                        set_message.set(format!("Ticket #{} updated.", updated.ticket_id));
                        set_ticket.set(Some(updated));
                    }
                    Err(err) => set_error.set(err.to_string()),
                }
            });
        }
    };

    let on_create_user = {
        let token = token.clone();
        move |_| {
            let Some(token) = token.clone() else {
                set_error.set("Missing auth token. Please sign in again.".to_string());
                return;
            };
            let Ok(role) = create_user_role.get().parse::<i32>() else {
                set_error.set("Role must be a valid integer enum value.".to_string());
                return;
            };
            let payload = CreateUserRequest {
                username: create_user_name.get(),
                password: create_user_password.get(),
                email: create_user_email.get(),
                display_name: create_user_display_name.get(),
                role,
            };
            set_error.set(String::new());
            set_message.set(String::new());
            spawn_local(async move {
                match api::create_user(&token, &payload).await {
                    Ok(()) => set_message.set("User created successfully.".to_string()),
                    Err(err) => set_error.set(err.to_string()),
                }
            });
        }
    };

    let on_sign_out = {
        let navigate = navigate.clone();
        move |_| {
            api::clear_token();
            navigate("/", leptos_router::NavigateOptions::default());
        }
    };

    view! {
        <div class="dashboard-container">
            <header class="dashboard-header">
                <h2>"InfoVulcan Console"</h2>
                <div class="header-actions">
                    <button class="btn-ghost" on:click=on_toggle_theme>
                        {move || if theme_sig.get() == theme::DARK { "☀ Light" } else { "🌙 Dark" }}
                    </button>
                    <button class="btn-secondary" on:click=on_sign_out>"Sign Out"</button>
                </div>
            </header>

            {move || token.is_none().then(|| view! {
                <div class="error-message">"No token found. Sign in on / first."</div>
            })}

            {move || (!message.get().is_empty()).then(|| view! {
                <div class="banner banner-ok">{message.get()}</div>
            })}
            {move || (!error.get().is_empty()).then(|| view! {
                <div class="banner banner-error">{error.get()}</div>
            })}

            // ── Analytics dashboard ───────────────────────────────────────────
            <section class="panel">
                <h3>"Analytics"</h3>
                <div class="stat-cards">
                    <div class="stat-card">
                        <div class="stat-value">{move || total.get()}</div>
                        <div class="stat-label">"Tickets in view"</div>
                    </div>
                </div>
                <div class="breakdowns">
                    <div class="breakdown">
                        <h4>"By status"</h4>
                        {move || {
                            let counts = status_counts.get();
                            if counts.is_empty() {
                                view! { <p class="muted">"No data — run a search."</p> }.into_view()
                            } else {
                                counts.into_iter().map(|(value, count)| view! {
                                    <div class="bar-row">
                                        <span class="bar-label">{domain::status_label(value)}</span>
                                        <span class="bar-count">{count}</span>
                                    </div>
                                }).collect_view()
                            }
                        }}
                    </div>
                    <div class="breakdown">
                        <h4>"By priority"</h4>
                        {move || {
                            let counts = priority_counts.get();
                            if counts.is_empty() {
                                view! { <p class="muted">"No data — run a search."</p> }.into_view()
                            } else {
                                counts.into_iter().map(|(value, count)| view! {
                                    <div class="bar-row">
                                        <span class="bar-label">{domain::priority_label(value)}</span>
                                        <span class="bar-count">{count}</span>
                                    </div>
                                }).collect_view()
                            }
                        }}
                    </div>
                </div>
            </section>

            // ── Search / filter ───────────────────────────────────────────────
            <section class="panel">
                <h3>"Search & Filter"</h3>
                <div class="filter-grid">
                    <div class="input-group">
                        <label for="filter-status">"Status"</label>
                        <select id="filter-status" on:change=move |ev| set_filter_status.set(event_target_value(&ev))>
                            <option value="any">"Any"</option>
                            {domain::STATUSES.iter().map(|(v, label)| view! {
                                <option value={v.to_string()}>{*label}</option>
                            }).collect_view()}
                        </select>
                    </div>
                    <div class="input-group">
                        <label for="filter-project">"Project"</label>
                        <input id="filter-project" type="text"
                            on:input=move |ev| set_filter_project.set(event_target_value(&ev)) />
                    </div>
                    <div class="input-group">
                        <label for="filter-assignee">"Assignee UUID"</label>
                        <input id="filter-assignee" type="text"
                            on:input=move |ev| set_filter_assignee.set(event_target_value(&ev)) />
                    </div>
                    <div class="input-group">
                        <label for="filter-account">"Account UUID"</label>
                        <input id="filter-account" type="text"
                            on:input=move |ev| set_filter_account.set(event_target_value(&ev)) />
                    </div>
                    <div class="input-group">
                        <label for="filter-limit">"Limit"</label>
                        <input id="filter-limit" type="text" prop:value=filter_limit
                            on:input=move |ev| set_filter_limit.set(event_target_value(&ev)) />
                    </div>
                    <div class="input-group checkbox">
                        <label for="filter-deleted">
                            <input id="filter-deleted" type="checkbox"
                                on:change=move |ev| set_filter_include_deleted.set(event_target_checked(&ev)) />
                            " Include deleted"
                        </label>
                    </div>
                </div>
                <button class="btn-primary" on:click=on_search>"Search Tickets"</button>

                {move || {
                    let rows = tickets.get();
                    (!rows.is_empty()).then(|| view! {
                        <table class="ticket-table">
                            <thead>
                                <tr>
                                    <th>"#"</th><th>"Title"</th><th>"Project"</th>
                                    <th>"Priority"</th><th>"Status"</th><th>"Next action"</th><th></th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows.into_iter().map(|t| {
                                    let row = t.clone();
                                    let load = load_into_form;
                                    view! {
                                        <tr>
                                            <td>{t.ticket_id}</td>
                                            <td>{t.title.clone()}</td>
                                            <td>{t.project.clone()}</td>
                                            <td>{domain::priority_label(t.priority)}</td>
                                            <td>{domain::status_label(t.status)}</td>
                                            <td>{t.next_action.as_ref().map_or_else(
                                                || "—".to_string(), crate::api::NextAction::summary)}</td>
                                            <td><button class="btn-ghost" on:click=move |_| load(row.clone())>"Edit"</button></td>
                                        </tr>
                                    }
                                }).collect_view()}
                            </tbody>
                        </table>
                    })
                }}
            </section>

            // ── Update loaded ticket (transition-validated) ───────────────────
            <section class="panel">
                <h3>"Update Ticket"</h3>
                <div class="input-group inline">
                    <label for="lookup-id">"Load by #"</label>
                    <input id="lookup-id" type="text" prop:value=lookup_id
                        on:input=move |ev| set_lookup_id.set(event_target_value(&ev)) />
                    <button class="btn-secondary" on:click=on_lookup>"Load"</button>
                </div>
                {move || ticket.get().map_or_else(
                    || view! { <p class="muted">"Select a ticket from the results above, or load one by number."</p> }.into_view(),
                    |t| {
                        let terminal = domain::is_terminal(t.status);
                        view! {
                            <p class="muted">{format!(
                                "Editing #{} — current status: {}",
                                t.ticket_id, domain::status_label(t.status))}</p>
                            {terminal.then(|| view! {
                                <p class="banner banner-warn">
                                    "This ticket is in a terminal state; no status changes are allowed."
                                </p>
                            })}
                        }.into_view()
                    },
                )}
                <div class="input-group">
                    <label for="update-title">"Title"</label>
                    <input id="update-title" type="text" prop:value=update_title
                        on:input=move |ev| set_update_title.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="update-project">"Project"</label>
                    <input id="update-project" type="text" prop:value=update_project
                        on:input=move |ev| set_update_project.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="update-priority">"Priority"</label>
                    <select id="update-priority" prop:value=update_priority
                        on:change=move |ev| set_update_priority.set(event_target_value(&ev))>
                        {domain::PRIORITIES.iter().map(|(v, label)| view! {
                            <option value={v.to_string()}>{*label}</option>
                        }).collect_view()}
                    </select>
                </div>
                <div class="input-group">
                    <label for="update-status">"Status (legal transitions only)"</label>
                    <select id="update-status" prop:value=update_status
                        on:change=move |ev| set_update_status.set(event_target_value(&ev))>
                        {move || update_status_options.get().into_iter().map(|s| view! {
                            <option value={s.to_string()}>{domain::status_label(s)}</option>
                        }).collect_view()}
                    </select>
                </div>
                <div class="input-group">
                    <label for="update-next-action">"Next action"</label>
                    <select id="update-next-action" prop:value=update_na_kind
                        on:change=move |ev| set_update_na_kind.set(event_target_value(&ev))>
                        <option value="unchanged">"Leave unchanged"</option>
                        <option value="none">"Clear (none)"</option>
                        <option value="follow_up">"Follow up"</option>
                        <option value="appointment">"Appointment"</option>
                        <option value="auto_close">"Auto-close"</option>
                    </select>
                </div>
                {move || matches!(update_na_kind.get().as_str(), "follow_up" | "appointment").then(|| view! {
                    <div class="input-group">
                        <label for="update-na-at">"When (unix seconds)"</label>
                        <input id="update-na-at" type="text" prop:value=update_na_at
                            on:input=move |ev| set_update_na_at.set(event_target_value(&ev)) />
                    </div>
                })}
                {move || (update_na_kind.get() == "auto_close").then(|| view! {
                    <div class="input-group">
                        <label for="update-na-schedule">"Auto-close schedule"</label>
                        <select id="update-na-schedule" prop:value=update_na_schedule
                            on:change=move |ev| set_update_na_schedule.set(event_target_value(&ev))>
                            <option value="end_of_day">"End of day"</option>
                            <option value="hours_24">"24 hours"</option>
                            <option value="hours_48">"48 hours"</option>
                            <option value="hours_72">"72 hours"</option>
                        </select>
                    </div>
                })}
                <button class="btn-primary" on:click=on_update_ticket>"Save Changes"</button>
            </section>

            // ── Create ticket ─────────────────────────────────────────────────
            <section class="panel">
                <h3>"Create Ticket"</h3>
                <div class="input-group">
                    <label for="new-ticket-title">"Title"</label>
                    <input id="new-ticket-title" type="text"
                        on:input=move |ev| set_new_title.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-ticket-project">"Project"</label>
                    <input id="new-ticket-project" type="text"
                        on:input=move |ev| set_new_project.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-ticket-account-uuid">"Account UUID"</label>
                    <input id="new-ticket-account-uuid" type="text"
                        on:input=move |ev| set_new_account_uuid.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-ticket-priority">"Priority"</label>
                    <select id="new-ticket-priority" prop:value=new_priority
                        on:change=move |ev| set_new_priority.set(event_target_value(&ev))>
                        {domain::PRIORITIES.iter().map(|(v, label)| view! {
                            <option value={v.to_string()}>{*label}</option>
                        }).collect_view()}
                    </select>
                </div>
                <button class="btn-primary" on:click=on_create_ticket>"Create Ticket"</button>
            </section>

            // ── Create user ───────────────────────────────────────────────────
            <section class="panel">
                <h3>"Create User"</h3>
                <div class="input-group">
                    <label for="new-user-username">"Username"</label>
                    <input id="new-user-username" type="text"
                        on:input=move |ev| set_create_user_name.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-user-password">"Password"</label>
                    <input id="new-user-password" type="password"
                        on:input=move |ev| set_create_user_password.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-user-email">"Email"</label>
                    <input id="new-user-email" type="text"
                        on:input=move |ev| set_create_user_email.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-user-display-name">"Display Name"</label>
                    <input id="new-user-display-name" type="text"
                        on:input=move |ev| set_create_user_display_name.set(event_target_value(&ev)) />
                </div>
                <div class="input-group">
                    <label for="new-user-role">"Role"</label>
                    <select id="new-user-role" prop:value=create_user_role
                        on:change=move |ev| set_create_user_role.set(event_target_value(&ev))>
                        {domain::ROLES.iter().map(|(v, label)| view! {
                            <option value={v.to_string()}>{*label}</option>
                        }).collect_view()}
                    </select>
                </div>
                <button class="btn-primary" on:click=on_create_user>"Create User"</button>
            </section>
        </div>
    }
}
