use crate::common::{query::MyQuery, subscription::MySubscription};
use lazy_graphql::{
    async_graphql::EmptyMutation, async_graphql::Schema, auth::token::EmptyActor,
    google_firebase::firebase::FirebaseClaims, service_state::ServiceState,
};

#[derive(Clone)]
pub struct AppState {
    claims: Option<FirebaseClaims<EmptyActor>>,
}

#[async_trait::async_trait]
impl ServiceState<MyQuery, EmptyMutation, MySubscription, EmptyActor> for AppState {
    const APP_NAME: &'static str = "TESTE SERVICE";

    async fn new() -> Self {
        AppState { claims: None }
    }

    fn get_graphql_schema(&self) -> Schema<MyQuery, EmptyMutation, MySubscription> {
        Schema::build(MyQuery, EmptyMutation, MySubscription).finish()
    }

    fn get_claims<'a>(&self) -> Result<&FirebaseClaims<EmptyActor>, &'a str> {
        match self.claims.as_ref() {
            Some(claims) => Ok(claims),
            None => Err("claims not found"),
        }
    }

    fn set_claims(&mut self, claims: Option<FirebaseClaims<EmptyActor>>) {
        self.claims = claims;
    }
}
