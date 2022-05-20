use crate::AppState;
pub(crate) use oauth2::basic::BasicClient;

use oauth2::reqwest::http_client;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IntrospectionUrl,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenIntrospectionResponse,
    TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct AuthMiddleware;

impl AuthMiddleware {
    pub fn new() -> Self {
        AuthMiddleware {}
    }
}

#[async_trait::async_trait]
impl<State: Clone + Send + Sync + 'static> tide::Middleware<State> for AuthMiddleware {
    async fn handle(
        &self,
        mut request: tide::Request<State>,
        next: tide::Next<'_, State>,
    ) -> tide::Result {
        let maybe_exp: Option<chrono::DateTime<chrono::offset::Utc>> = request.session().get("exp");
        if let Some(exp) = maybe_exp {
            let now = chrono::offset::Utc::now();
            if exp > now {
                info!("authenticated session found");
                let response = next.run(request).await;
                Ok(response)
            } else {
                info!("expired session, redirecting");
                request.session_mut().remove("exp");

                Ok(tide::Redirect::new("/oauth/login").into())
            }
        } else {
            info!("authenticated NOT found, redirecting");
            Ok(tide::Redirect::new("/oauth/login").into())
        }
    }
}

pub fn configure_oauth(
    client_id: &str,
    client_secret: &str,
    client_url: &str,
    server_url: &str,
) -> BasicClient {
    let client_id = ClientId::new(client_id.to_string());
    let secret = ClientSecret::new(client_secret.to_string());
    let auth_url = AuthUrl::new(format!("{}/ui/oauth2", server_url)).unwrap();
    let token_url = TokenUrl::new(format!("{}/oauth2/token", server_url)).unwrap();
    let intro_url =
        IntrospectionUrl::new(format!("{}/oauth2/token/introspect", server_url)).unwrap();
    let redir_url = RedirectUrl::new(format!("{}/oauth/response", client_url)).unwrap();

    BasicClient::new(client_id, Some(secret), auth_url, Some(token_url))
        .set_redirect_uri(redir_url)
        .set_introspection_uri(intro_url)
}

// Not authenticated - kick of the redir to oauth.
pub(crate) async fn login_view(mut request: tide::Request<Arc<AppState>>) -> tide::Result {
    let (pkce_code_challenge, pkce_code_verifier) = PkceCodeChallenge::new_random_sha256();

    debug!("challenge -> {:?}", pkce_code_challenge.as_str());
    debug!("secret -> {:?}", pkce_code_verifier.secret());

    let (auth_url, csrf_token) = request
        .state()
        .oauth
        .as_ref()
        .ok_or_else(|| tide::Error::new(403 as u16, anyhow::Error::msg("Forbidden")))?
        .authorize_url(CsrfToken::new_random)
        .add_scope(Scope::new("read".to_string()))
        .set_pkce_challenge(pkce_code_challenge)
        .url();

    // We can stash the verifier in the session.
    let session = request.session_mut();
    session
        .insert("pkce_code_verifier", &pkce_code_verifier)
        .unwrap();
    session.insert("csrf_token", &csrf_token).unwrap();

    info!("starting oauth");
    Ok(tide::Redirect::new(auth_url.as_str()).into())
}

#[derive(Debug, Serialize, Deserialize)]
struct OauthResp {
    state: CsrfToken,
    code: AuthorizationCode,
}

// Handle the response
pub(crate) async fn oauth_view(mut request: tide::Request<Arc<AppState>>) -> tide::Result {
    // How do we get the params out?
    let resp: OauthResp = request.query().map_err(|e| {
        error!("{:?}", e);
        e
    })?;

    debug!("resp -> {:?}", resp);

    // get the verifier and csrf token
    let session = request.session();
    let pkce_code_verifier: PkceCodeVerifier =
        session.get("pkce_code_verifier").ok_or_else(|| {
            error!("pkce");
            tide::Error::new(500 as u16, anyhow::Error::msg("pkce"))
        })?;
    debug!("secret -> {:?}", pkce_code_verifier.secret());
    let csrf_token: CsrfToken = session.get("csrf_token").ok_or_else(|| {
        error!("csrf");
        tide::Error::new(500 as u16, anyhow::Error::msg("csrf"))
    })?;

    // Compare state to csrf token.
    if csrf_token.secret() != resp.state.secret() {
        error!("csrf validation");
        // give an error?
        return Ok(tide::Response::builder(tide::StatusCode::Conflict)
            .body("csrf failure")
            .build());
    }

    let r_token = request
        .state()
        .oauth
        .as_ref()
        .ok_or_else(|| tide::Error::new(403 as u16, anyhow::Error::msg("Forbidden")))?
        .exchange_code(resp.code)
        .set_pkce_verifier(pkce_code_verifier)
        // .request_async(async_http_client)
        .request(http_client);

    let tr = match r_token {
        Ok(tr) => {
            debug!("{:?}", tr.access_token());
            debug!("{:?}", tr.token_type());
            debug!("{:?}", tr.scopes());
            tr
        }
        Err(e) => {
            error!("oauth2 token request failure - {:?}", e);
            return Ok(
                tide::Response::builder(tide::StatusCode::InternalServerError)
                    .body("token request failure")
                    .build(),
            );
        }
    };

    let intro_result = request
        .state()
        .oauth
        .as_ref()
        .ok_or_else(|| tide::Error::new(403 as u16, anyhow::Error::msg("Forbidden")))?
        .introspect(tr.access_token())
        .unwrap()
        .request(http_client);

    info!("{:?}", intro_result);

    match intro_result {
        Ok(ir) => {
            let exp = ir.exp().unwrap();
            let username = ir.username().unwrap();

            request.session_mut().insert("exp", exp);
            request.session_mut().insert("username", username);

            Ok(tide::Redirect::new("/_admin").into())
        }
        Err(e) => {
            error!("oauth2 token request failure - {:?}", e);
            Ok(
                tide::Response::builder(tide::StatusCode::InternalServerError)
                    .body("token request failure")
                    .build(),
            )
        }
    }
}
