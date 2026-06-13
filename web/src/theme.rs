//! Dark/light theme handling, persisted in `localStorage`.
//!
//! The active theme is written to `localStorage` and reflected as a `data-theme` attribute on the
//! document root (`<html>`), which `style.css` keys off (`:root` = dark default, `[data-theme=
//! "light"]` = light overrides). Defaults to dark.

const THEME_KEY: &str = "infovulcan_theme";

pub const DARK: &str = "dark";
pub const LIGHT: &str = "light";

fn storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok()?
}

/// The persisted theme, or [`DARK`] if unset/unavailable.
#[must_use]
pub fn current_theme() -> String {
    storage()
        .and_then(|s| s.get_item(THEME_KEY).ok().flatten())
        .filter(|t| t == DARK || t == LIGHT)
        .unwrap_or_else(|| DARK.to_string())
}

/// Persist `theme` and apply it to the document root.
pub fn set_theme(theme: &str) {
    if let Some(s) = storage() {
        let _ = s.set_item(THEME_KEY, theme);
    }
    apply(theme);
}

/// Reflect `theme` onto `<html data-theme=…>` so CSS can react.
pub fn apply(theme: &str) {
    if let Some(doc) = web_sys::window().and_then(|w| w.document()) {
        if let Some(root) = doc.document_element() {
            let _ = root.set_attribute("data-theme", theme);
        }
    }
}

/// Apply whatever theme is currently persisted (call once on startup).
pub fn apply_current() {
    apply(&current_theme());
}

/// Flip dark↔light, persist, apply, and return the new theme.
pub fn toggle() -> String {
    let next = if current_theme() == DARK { LIGHT } else { DARK }.to_string();
    set_theme(&next);
    next
}
