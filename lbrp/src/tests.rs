#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::middleware::Claims;
    use crate::{env_or_default, jwt_secret_from_env, parse_listen_addr, resolve_backend_addrs};
    use jsonwebtoken::{EncodingKey, Header, encode};

    #[test]
    fn test_claims_serialization() {
        let claims = Claims {
            sub: "user123".to_string(),
            role: "admin".to_string(),
            exp: 10_000_000_000,
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"secret"),
        )
        .expect("Failed to encode token");

        assert!(!token.is_empty());
    }

    #[test]
    fn test_parse_listen_addr_defaults_and_invalid_value() {
        let default_addr = parse_listen_addr(None).expect("default addr");
        assert_eq!(default_addr.to_string(), "0.0.0.0:8080");

        assert!(parse_listen_addr(Some("not-an-addr".to_string())).is_err());
    }

    #[test]
    fn test_env_or_default_and_jwt_secret_helpers() {
        assert_eq!(
            env_or_default(None, "http://auth:8082"),
            "http://auth:8082".to_string()
        );
        assert_eq!(
            env_or_default(Some("http://custom:1".to_string()), "ignored"),
            "http://custom:1".to_string()
        );

        // A missing or empty secret is a hard error (no weak default).
        assert!(jwt_secret_from_env(None).is_err());
        assert!(jwt_secret_from_env(Some(String::new())).is_err());
        assert_eq!(
            jwt_secret_from_env(Some("abc".to_string())).expect("secret present"),
            b"abc".to_vec()
        );
    }

    #[test]
    fn parse_listen_addr_accepts_valid_some_value() {
        let addr =
            parse_listen_addr(Some("127.0.0.1:9090".to_string())).expect("valid addr parses");
        assert_eq!(addr.port(), 9090);
    }

    #[test]
    fn resolve_backend_addrs_uses_defaults_when_all_none() {
        let (auth, admin, custodian) = resolve_backend_addrs(None, None, None);
        assert_eq!(auth, "http://auth:8082");
        assert_eq!(admin, "http://admin:8083");
        assert_eq!(custodian, "http://custodian-leader:8081");
    }

    #[test]
    fn resolve_backend_addrs_uses_provided_values() {
        let (auth, admin, custodian) = resolve_backend_addrs(
            Some("http://custom-auth:1".into()),
            Some("http://custom-admin:2".into()),
            Some("http://custom-custodian:3".into()),
        );
        assert_eq!(auth, "http://custom-auth:1");
        assert_eq!(admin, "http://custom-admin:2");
        assert_eq!(custodian, "http://custom-custodian:3");
    }

    #[test]
    fn resolve_backend_addrs_partial_override() {
        let (auth, admin, custodian) =
            resolve_backend_addrs(Some("http://my-auth:999".into()), None, None);
        assert_eq!(auth, "http://my-auth:999");
        assert_eq!(admin, "http://admin:8083");
        assert_eq!(custodian, "http://custodian-leader:8081");
    }
}
