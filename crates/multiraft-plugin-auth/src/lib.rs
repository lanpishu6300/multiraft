//! Optional Bearer admin auth (A12 extension).

use multiraft_core::AuthPlugin;
use multiraft_core::Plugin;

/// When `token` is empty, all requests are allowed (lab default).
#[derive(Clone, Debug)]
pub struct BearerTokenAuth {
    token: String,
}

impl BearerTokenAuth {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    pub fn from_env() -> Self {
        Self::new(std::env::var("MULTIRAFT_ADMIN_TOKEN").unwrap_or_default())
    }
}

impl Plugin for BearerTokenAuth {
    fn name(&self) -> &'static str {
        "bearer-auth"
    }
}

impl AuthPlugin for BearerTokenAuth {
    fn authorize_admin(&self, authorization: Option<&str>) -> bool {
        if self.token.is_empty() {
            return true;
        }
        authorization.is_some_and(|h| h == format!("Bearer {}", self.token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_token_allows_all() {
        let auth = BearerTokenAuth::new("");
        assert!(auth.authorize_admin(None));
        assert!(auth.authorize_admin(Some("Bearer anything")));
    }

    #[test]
    fn token_requires_bearer_match() {
        let auth = BearerTokenAuth::new("secret");
        assert!(!auth.authorize_admin(None));
        assert!(!auth.authorize_admin(Some("secret")));
        assert!(auth.authorize_admin(Some("Bearer secret")));
    }
}
