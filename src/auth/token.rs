//! Substitui o token.rs antigo: toda a lógica de Google/Firebase agora vive no
//! crate `google-firebase`; aqui sobra só a cola com o Axum e com o seu erro.
//!
//! Em err.rs (ou aqui) adicione a conversão do erro do SDK:
//!     impl From<google_firebase::Error> for ServiceError { ... }
//! Seus ActorClaims continuam no app (firebase_claims.rs); só ServiceAccountKey,
//! GoogleAuthClaims, GoogleTokenResponse, FirebaseClaims e FirebaseInfo saem de lá.

use std::sync::LazyLock;

use axum::{extract::FromRequestParts, http::request::Parts};
use google_firebase::firebase::{FirebaseAuth, FirebaseClaims};
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::ServiceError;

// pub type AppClaims = FirebaseClaims<ActorClaims>;

/// Criado uma vez: cacheia certificados do Firebase e o access token da service account.
static FIREBASE: LazyLock<FirebaseAuth> =
    LazyLock::new(|| FirebaseAuth::from_env().expect("Firebase mal configurado"));

impl From<google_firebase::Error> for ServiceError {
    fn from(e: google_firebase::Error) -> Self {
        ServiceError::Validation(e.to_string())
    }
}

#[derive(Deserialize, Clone)]
pub struct EmptyActor;

pub struct Token;

impl Token {
    pub async fn set_custom_attributes<C: Serialize>(
        claims: &FirebaseClaims<C>,
        actor: C,
    ) -> Result<(), ServiceError> {
        Ok(FIREBASE.set_custom_claims(&claims.user_id, &actor).await?)
    }

    pub async fn validade_as_firebase<ActorClaims: DeserializeOwned + Send + Sync + 'static>(
        token: &str,
    ) -> Result<FirebaseClaims<ActorClaims>, ServiceError> {
        Ok(FIREBASE.verify_id_token(token).await?)
    }
}

pub struct BearerToken<ActorClaims: DeserializeOwned + Send + Sync + 'static>(
    pub Option<FirebaseClaims<ActorClaims>>,
);

impl<S, ActorClaims: DeserializeOwned + Send + Sync + 'static> FromRequestParts<S>
    for BearerToken<ActorClaims>
where
    S: Send + Sync,
{
    type Rejection = String;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let Some(token) = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        else {
            return Ok(BearerToken(None));
        };

        match Token::validade_as_firebase::<ActorClaims>(token).await {
            Ok(claims) => Ok(BearerToken(Some(claims))),
            Err(e) => {
                eprintln!("Erro ao validar token: {e:?}");
                Ok(BearerToken(None))
            }
        }
    }
}
