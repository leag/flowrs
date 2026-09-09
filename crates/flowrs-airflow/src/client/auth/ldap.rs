use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use log::info;
use regex::Regex;
use reqwest::cookie::{CookieStore, Jar};
use reqwest::{RequestBuilder, Url};

use super::AuthProvider;
use crate::error::{AirflowError, Result};

/// How long a fetched JWT is reused before the LDAP login flow runs again.
const TOKEN_TTL: Duration = Duration::from_secs(60);

/// Authenticates LDAP users by replicating Flask-AppBuilder's web login form,
/// then exchanging the resulting session for the JWT the API expects.
pub struct LdapAuthProvider {
    base_url: Url,
    username: String,
    password: String,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl fmt::Debug for LdapAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LdapAuthProvider")
            .field("base_url", &self.base_url)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl LdapAuthProvider {
    pub fn new(base_url: Url, username: String, password: String) -> Self {
        Self {
            base_url,
            username,
            password,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// Runs the three-step login dance and returns the JWT from the `_token`
    /// cookie.
    async fn fetch_token(&self) -> anyhow::Result<String> {
        // A private, explicit cookie jar (rather than the default
        // `.cookie_store(true)`) so the JWT can be read back out of it once
        // the exchange request has run.
        let jar = Arc::new(Jar::default());
        let client = reqwest::Client::builder()
            .cookie_provider(Arc::clone(&jar))
            .build()
            .context("failed to build LDAP login HTTP client")?;

        let login_url = self
            .base_url
            .join("auth/login/")
            .context("failed to build login URL")?;

        // 1. GET the login form to pick up a CSRF token + session cookie.
        let html = client
            .get(login_url.clone())
            .send()
            .await
            .context("failed to fetch login form")?
            .text()
            .await
            .context("failed to read login form body")?;

        let csrf_re =
            Regex::new(r#"name="csrf_token"[^>]*value="([^"]+)""#).expect("static regex is valid");
        let csrf_token = csrf_re
            .captures(&html)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| {
                anyhow::anyhow!("no csrf_token found in login form; login page may have changed")
            })?;

        // 2. POST credentials (LDAP validated here by Flask-AppBuilder).
        let response = client
            .post(login_url.clone())
            .header("Referer", login_url.as_str())
            .form(&[
                ("csrf_token", csrf_token.as_str()),
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .await
            .context("failed to submit login form")?;

        if response.url().path().contains("auth/login") {
            anyhow::bail!("LDAP login failed: invalid credentials");
        }

        // 3. Exchange the session for a JWT via the API's own login-redirect
        //    dance; the resulting `_token` cookie holds the JWT.
        let next = self
            .base_url
            .join("login/")
            .context("failed to build 'next' URL")?;
        let mut exchange_url = self
            .base_url
            .join("api/v2/auth/login")
            .context("failed to build exchange URL")?;
        exchange_url
            .query_pairs_mut()
            .append_pair("next", next.as_str());

        client
            .get(exchange_url)
            .send()
            .await
            .context("failed to exchange session for a JWT")?;

        jar.cookies(&self.base_url)
            .and_then(|header| {
                header.to_str().ok().and_then(|cookies| {
                    cookies.split(';').find_map(|pair| {
                        let pair = pair.trim();
                        pair.strip_prefix("_token=").map(str::to_string)
                    })
                })
            })
            .ok_or_else(|| anyhow::anyhow!("no _token cookie set; exchange flow may have changed"))
    }
}

#[async_trait]
impl AuthProvider for LdapAuthProvider {
    async fn authenticate(&self, request: RequestBuilder) -> Result<RequestBuilder> {
        let mut cached = self.cached.lock().await;

        let fresh = cached
            .as_ref()
            .is_some_and(|(_, fetched)| fetched.elapsed() < TOKEN_TTL);

        if !fresh {
            info!("🔑 LDAP Auth: refreshing JWT for {}", self.username);
            let token = self
                .fetch_token()
                .await
                .map_err(|e| AirflowError::auth("LDAP", &e))?;
            *cached = Some((token, Instant::now()));
        }

        let (token, _) = cached.as_ref().expect("token cached above");
        Ok(request.bearer_auth(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const LOGIN_FORM_HTML: &str = r#"
        <form>
          <input type="hidden" name="csrf_token" value="test-csrf-token">
        </form>
    "#;

    fn bearer(request: RequestBuilder) -> String {
        request
            .build()
            .unwrap()
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    fn get(server: &MockServer) -> RequestBuilder {
        reqwest::Client::new().get(format!("{}/api/v2/dags", server.uri()))
    }

    #[tokio::test]
    async fn authenticates_via_the_ldap_form_login_flow() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/auth/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LOGIN_FORM_HTML))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/auth/login/"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("Location", format!("{}/", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;

        // The redirect above is followed as a GET to "/" by the HTTP client.
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v2/auth/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Set-Cookie", "_token=jwt-value-123; Path=/"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let base_url: Url = format!("{}/", server.uri()).parse().unwrap();
        let provider = LdapAuthProvider::new(base_url, "alice".to_string(), "secret".to_string());

        let request = provider.authenticate(get(&server)).await.unwrap();
        assert_eq!(bearer(request), "Bearer jwt-value-123");
    }

    #[tokio::test]
    async fn fails_when_credentials_are_rejected() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/auth/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LOGIN_FORM_HTML))
            .mount(&server)
            .await;

        // Invalid credentials: FAB re-renders the login page instead of
        // redirecting elsewhere.
        Mock::given(method("POST"))
            .and(path("/auth/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LOGIN_FORM_HTML))
            .mount(&server)
            .await;

        let base_url: Url = format!("{}/", server.uri()).parse().unwrap();
        let provider =
            LdapAuthProvider::new(base_url, "alice".to_string(), "wrong-password".to_string());

        let result = provider.authenticate(get(&server)).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("LDAP"),
            "error should identify the LDAP provider"
        );
    }

    #[tokio::test]
    async fn fails_when_the_login_form_has_no_csrf_token() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/auth/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>no form here</html>"))
            .mount(&server)
            .await;

        let base_url: Url = format!("{}/", server.uri()).parse().unwrap();
        let provider = LdapAuthProvider::new(base_url, "alice".to_string(), "secret".to_string());

        let result = provider.authenticate(get(&server)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn caches_the_token_and_runs_the_login_flow_only_once() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/auth/login/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(LOGIN_FORM_HTML))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/auth/login/"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("Location", format!("{}/", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v2/auth/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Set-Cookie", "_token=jwt-value-123; Path=/"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let base_url: Url = format!("{}/", server.uri()).parse().unwrap();
        let provider = LdapAuthProvider::new(base_url, "alice".to_string(), "secret".to_string());

        // Two authentications within the TTL should reuse the first token,
        // hitting the login endpoints only once (checked by `.expect(1)`
        // above, verified on drop).
        let _ = provider.authenticate(get(&server)).await.unwrap();
        let _ = provider.authenticate(get(&server)).await.unwrap();
    }
}
