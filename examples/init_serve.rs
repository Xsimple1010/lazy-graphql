use lazy_graphql::async_graphql::EmptyMutation;
use lazy_graphql::auth::token::EmptyActor;
use lazy_graphql::cron_job_setup::SubgraphJob;
use lazy_graphql::subgraph_service::SubgraphService;

use crate::common::query::MyQuery;
use crate::common::state::AppState;
use crate::common::subscription::{MySubscription, send_message_cron_job};

mod common;

#[tokio::main]
async fn main() {
    Service::builder().await
}

pub struct Service;

#[async_trait::async_trait]
impl SubgraphService<MyQuery, EmptyMutation, MySubscription, AppState, EmptyActor> for Service {
    async fn set_cron_job<'a>()
    -> Vec<SubgraphJob<MyQuery, EmptyMutation, MySubscription, AppState, EmptyActor>> {
        vec![SubgraphJob {
            name: "Send message using graphql subscription",
            handler: send_message_cron_job,
            schedule: "*/3 * * * * *",
            _phantom: Default::default(),
        }]
    }
}
