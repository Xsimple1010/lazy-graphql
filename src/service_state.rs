use crate::env_vars::{self, get_aws_region, get_aws_s3_bucket_name, get_redis_url};
use crate::error::ServiceError;
use crate::graphql_subscription_operation::on_connection_init;
use crate::kafka_common::setup::create_producer;
use crate::redis_notification::RedisNotification;
use async_graphql::{Context, Request, Response, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLWebSocket};
use aws_s3_manager::s3::AwsS3Manager;
use axum::extract::ws::WebSocket;
use google_firebase::firebase::FirebaseClaims;
use rdkafka::producer::FutureProducer;
use redis::aio::ConnectionManager;
use sea_orm::{Database, DatabaseConnection};
use serde::de::DeserializeOwned;
use std::time::Duration;
use tokio::sync::OnceCell;

static DB: OnceCell<DatabaseConnection> = OnceCell::const_new();
static REDIS_CONNECTION_MANAGER: OnceCell<ConnectionManager> = OnceCell::const_new();
static REDIS_NOTIFICATION: OnceCell<RedisNotification> = OnceCell::const_new();
// static KEY_VAULT: OnceCell<KeyVaultEnvCredential> = OnceCell::const_new();
static KAFKA_PRODUCE: OnceCell<FutureProducer> = OnceCell::const_new();
static S3MANAGER: OnceCell<AwsS3Manager> = OnceCell::const_new();

pub async fn create_db_connection() -> DatabaseConnection {
    let data_base_url = env_vars::get_db_url();
    let mut opt = sea_orm::ConnectOptions::new(data_base_url);
    opt.max_connections(100)
        .min_connections(5)
        .connect_timeout(Duration::from_secs(8))
        .idle_timeout(Duration::from_secs(8))
        .max_lifetime(Duration::from_secs(8))
        .sqlx_logging(false);
    Database::connect(opt)
        .await
        .expect("Fail to initialize database connection")
}

#[async_trait::async_trait]
pub trait ServiceState<
    Query: async_graphql::ObjectType + 'static,
    Mutation: async_graphql::ObjectType + 'static,
    Subscription: SubscriptionType + 'static,
    ActorClaims: DeserializeOwned + Send + Sync + 'static,
>: Clone + Send + Sync + 'static
{
    const APP_NAME: &'static str;

    async fn new() -> Self;

    fn get_graphql_schema(&self) -> Schema<Query, Mutation, Subscription>;

    fn get_claims<'a>(&self) -> Result<&FirebaseClaims<ActorClaims>, &'a str>;

    fn set_claims(&mut self, claims: Option<FirebaseClaims<ActorClaims>>);

    async fn execute_common_op_with_schema(&self, req: Request) -> Response {
        self.get_graphql_schema().execute(req).await
    }

    fn get_app_name(&self) -> &'static str {
        Self::APP_NAME
    }

    // async fn get_default_key_vault() -> KeyVaultEnvCredential {
    //     KEY_VAULT
    //         .get_or_init(async || KeyVaultEnvCredential {
    //             client_id: Arc::new(get_azure_client_id()),
    //             client_secret: Arc::new(get_azure_client_secret()),
    //             tenant_id: Arc::new(get_azure_tenant_id()),
    //             vault_url: Arc::new(get_azure_vault_url()),
    //         })
    //         .await
    //         .clone()
    // }

    async fn get_s3_manager(&self) -> AwsS3Manager {
        S3MANAGER
            .get_or_init(async || AwsS3Manager::new(get_aws_s3_bucket_name(), get_aws_region()))
            .await
            .clone()
    }

    async fn get_db_ref<'a>(&self) -> &'a DatabaseConnection {
        DB.get_or_init(create_db_connection).await
    }

    async fn get_db() -> DatabaseConnection {
        DB.get_or_init(create_db_connection).await.clone()
    }

    fn get_from_context<'a>(ctx: &'a Context<'_>) -> Result<&'a Self, ServiceError> {
        ctx.data::<Self>()
            .map_err(|_| ServiceError::Validation("AppState not found in context".into()))
    }

    async fn execute_socket(&self, stream: WebSocket, protocol: GraphQLProtocol) {
        let state = self.clone();
        GraphQLWebSocket::new(stream, self.get_graphql_schema(), protocol)
            .on_connection_init(move |value| on_connection_init(value, state.clone()))
            .serve()
            .await;
    }

    async fn get_redis_notification(&self) -> RedisNotification {
        let app_name = self.get_app_name();
        REDIS_NOTIFICATION
            .get_or_init(async || {
                let client =
                    redis::Client::open(format!("{}/?protocol=resp3", get_redis_url())).unwrap();
                RedisNotification::new(client, &app_name).await.unwrap()
            })
            .await
            .clone()
    }

    async fn get_redis_connection(&self) -> ConnectionManager {
        REDIS_CONNECTION_MANAGER
            .get_or_init(async || {
                let client = redis::Client::open(get_redis_url()).unwrap();
                ConnectionManager::new(client).await.unwrap()
            })
            .await
            .clone()
    }

    async fn get_kafka_produce(&self) -> FutureProducer {
        KAFKA_PRODUCE
            .get_or_init(async || create_producer())
            .await
            .clone()
    }
}
