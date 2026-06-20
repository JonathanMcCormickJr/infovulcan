use crate::api;
use leptos::*;
use leptos_router::use_navigate;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn Login() -> impl IntoView {
    let (username, set_username) = create_signal(String::new());
    let (password, set_password) = create_signal(String::new());
    let (error, set_error) = create_signal(String::new());
    let (is_loading, set_is_loading) = create_signal(false);
    let navigate = use_navigate();

    let on_submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();

        let username_value = username.get();
        let password_value = password.get();
        let navigate = navigate.clone();

        set_is_loading.set(true);
        set_error.set(String::new());

        spawn_local(async move {
            match api::login(username_value, password_value).await {
                Ok(()) => {
                    navigate("/tickets", leptos_router::NavigateOptions::default());
                }
                Err(err) => {
                    set_error.set(err.to_string());
                }
            }
            set_is_loading.set(false);
        });
    };

    view! {
        <div class="auth-container">
            <div class="auth-box">
                <h2>"Sign in to InfoVulcan"</h2>
                <form on:submit=on_submit>
                    <div class="input-group">
                        <label for="username">"Username"</label>
                        <input
                            id="username"
                            type="text"
                            on:input=move |ev| set_username.set(event_target_value(&ev))
                            prop:value=username
                            placeholder="username"
                        />
                    </div>
                    <div class="input-group">
                        <label for="password">"Password"</label>
                        <input
                            id="password"
                            type="password"
                            on:input=move |ev| set_password.set(event_target_value(&ev))
                            prop:value=password
                            placeholder="••••••••"
                        />
                    </div>
                    <button type="submit" class="btn-primary" disabled=move || is_loading.get()>
                        {move || if is_loading.get() { "Signing In..." } else { "Sign In" }}
                    </button>
                    {move || (!error.get().is_empty()).then(|| view! {
                        <div class="error-message">{error.get()}</div>
                    })}
                </form>
            </div>
        </div>
    }
}
