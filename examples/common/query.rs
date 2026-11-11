use async_graphql::{Context, Object};
use lazy_graphql::service_state::ServiceState;

use crate::common::state::AppState;

pub struct MyQuery;

#[Object]
impl MyQuery {
    async fn howdy(&self, ctx: &Context<'_>) -> Result<String, String> {
        let ctx = AppState::get_from_context(&ctx)?;

        let claims = ctx.get_claims()?;

        Ok(match &claims.email {
            Some(email) => email.clone(),
            None => "sem email".to_owned(),
        })
    }
}
