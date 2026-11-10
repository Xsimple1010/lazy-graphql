use crate::cron_job_setup::{SubgraphJob, start_jobs};
use crate::error::ServiceError;
use crate::graphql_common_operation::graphql_common_operation;
use crate::graphql_subscription_operation::graphql_subscription_operation;
use crate::service_state::ServiceState;
use async_graphql::SubscriptionType;
use async_graphql::http::GraphiQLSource;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Router, response};
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

#[async_trait::async_trait]
pub trait SubgraphService<
    Query: async_graphql::ObjectType + 'static,
    Mutation: async_graphql::ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    S: ServiceState<Query, Mutation, Subscription, ActorClaims> + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
>
{
    async fn builder() {
        dotenv::dotenv().ok();
        let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());

        let state = Self::get_state().await;

        let router = Self::attach_custom_router(state.clone());
        let router = Self::attach_router(state.clone(), router);

        let jobs = Self::set_cron_job().await;
        start_jobs(jobs, state.clone()).await;
        let addr = format!("0.0.0.0:{port}");

        // Self::set_kafka_job(state.clone()).await;

        let listener = TcpListener::bind(&addr).await.unwrap();

        println!("🚀 {} Listening on http://{}", S::APP_NAME, addr);

        Self::before_start(state).await.unwrap();

        axum::serve(listener, router).await.unwrap();
    }

    async fn before_start(_state: S) -> Result<(), ServiceError> {
        Ok(())
    }

    // async fn set_kafka_job(_state: S) {}

    async fn get_state() -> S {
        S::new().await
    }

    async fn set_cron_job<'a>() -> Vec<SubgraphJob<Query, Mutation, Subscription, S, ActorClaims>> {
        vec![]
    }

    fn attach_router(state: S, router: Router<S>) -> Router {
        router
            .route("/", get(graphiql))
            .route(
                "/graphql",
                post(graphql_common_operation::<Query, Mutation, Subscription, S, ActorClaims>),
            )
            .route(
                "/ws",
                get(
                    graphql_subscription_operation::<Query, Mutation, Subscription, S, ActorClaims>,
                ),
            )
            .with_state(state)
            .layer(CorsLayer::permissive())
    }

    fn attach_custom_router(_state: S) -> Router<S> {
        Router::new()
    }
}

async fn graphiql() -> impl IntoResponse {
    response::Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/ws")
            .finish(),
    )
}
