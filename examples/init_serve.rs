use subgraph_common::async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema};
use subgraph_common::google_firebase::firebase::FirebaseClaims;
use subgraph_common::auth::token::EmptyActor;
use subgraph_common::service_state::ServiceState;
use subgraph_common::subgraph_service::SubgraphService;

#[tokio::main]
async fn main() {
    Service::builder().await
}

struct Query;

#[Object]
impl Query {
    async fn howdy(&self, ctx: &Context<'_>) -> Result<String, String> {
        let ctx = AppState::get_from_context(&ctx)?;

        let claims = ctx.get_claims()?;

        Ok(match &claims.email {
            Some(email) => email.clone(),
            None => "sem email".to_owned(),
        })
    }
}

#[derive(Clone)]
struct AppState {
    claims: Option<FirebaseClaims<EmptyActor>>,
}

#[async_trait::async_trait]
impl ServiceState<Query, EmptyMutation, EmptySubscription, EmptyActor> for AppState {
    const APP_NAME: &'static str = "TESTE SERVICE";

    async fn new() -> Self {
        AppState { claims: None }
    }

    fn get_graphql_schema(&self) -> Schema<Query, EmptyMutation, EmptySubscription> {
        Schema::build(Query, EmptyMutation, EmptySubscription).finish()
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

pub struct Service;

#[async_trait::async_trait]
impl SubgraphService<Query, EmptyMutation, EmptySubscription, AppState, EmptyActor> for Service {}

