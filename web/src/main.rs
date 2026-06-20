#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]
// Framework-driven exceptions to pedantic, scoped to the WASM frontend:
// - Leptos uses prelude globs (`use leptos::*`) — the framework's documented idiom.
// - Leptos `#[component]` view functions are large `view!` trees by nature.
// - DTO field names mirror the REST/proto wire contract (e.g. `Ticket.ticket_id`); renaming
//   them to satisfy the lint would break serde (de)serialization.
#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::struct_field_names
)]

mod api;
mod components;
mod domain;
mod theme;
use components::login::Login;
use components::ticket_list::TicketList;
use leptos::*;
use leptos_router::*;

fn main() {
    console_error_panic_hook::set_once();
    // Reflect the persisted theme before the first paint.
    theme::apply_current();
    mount_to_body(|| view! { <App/> });
}

#[component]
fn App() -> impl IntoView {
    view! {
        <Router>
            <main>
                <Routes>
                    <Route path="/" view=|| view! { <Login/> }/>
                    <Route path="/tickets" view=|| view! { <TicketList/> }/>
                </Routes>
            </main>
        </Router>
    }
}
