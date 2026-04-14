// src/auth/basic.rs
//
// HTTP Basic authentication middleware for axum.

use axum::body::Body;
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;

/// Configuration for HTTP Basic authentication.
#[derive(Clone, Debug)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

/// Axum middleware function that enforces HTTP Basic authentication.
///
/// If the `Authorization` header is absent or the credentials are wrong, the
/// request is rejected with `401 Unauthorized` and a `WWW-Authenticate` header
/// prompting for credentials.
pub async fn basic_auth_middleware(
    headers: HeaderMap,
    config: axum::extract::Extension<BasicAuthConfig>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if check_credentials(&headers, &config) {
        next.run(request).await
    } else {
        unauthorized_response()
    }
}

fn check_credentials(headers: &HeaderMap, config: &BasicAuthConfig) -> bool {
    let Some(auth_header) = headers.get(header::AUTHORIZATION) else {
        return false;
    };

    let Ok(auth_str) = auth_header.to_str() else {
        return false;
    };

    let Some(encoded) = auth_str.strip_prefix("Basic ") else {
        return false;
    };

    let Ok(decoded_bytes) = B64.decode(encoded.trim()) else {
        return false;
    };

    let Ok(decoded) = std::str::from_utf8(&decoded_bytes) else {
        return false;
    };

    let Some((user, pass)) = decoded.split_once(':') else {
        return false;
    };

    user == config.username && pass == config.password
}

fn unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            "Basic realm=\"HydroCube\", charset=\"UTF-8\"",
        )],
        "Unauthorized",
    )
        .into_response()
}
