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
    mount_to_body(|| view! { <App/> })
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
