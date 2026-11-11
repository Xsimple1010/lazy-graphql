use crate::common::state::AppState;
use lazy_graphql::{
    async_graphql::{Context, Subscription},
    cron_job_setup::JobHandler,
    lazy_graphql_macros::subscription_message,
    redis_notification::NotificationChannel,
    service_state::ServiceState,
    tokio_stream::Stream,
};

pub struct MySubscription;

#[subscription_message]
pub struct Message {
    pub message: String,
}

pub struct MessageChannel {
    pub id: String,
}

impl NotificationChannel for MessageChannel {
    type Payload = Message;

    fn channel_name(&self) -> String {
        format!("channel:{}", self.id)
    }
}

#[Subscription]
impl MySubscription {
    async fn event_listener(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Message>, String> {
        let state = AppState::get_from_context(&ctx)?.clone();

        MessageChannel {
            id: "user_id".to_string(),
        }
        .subscribe(state.get_redis_notification().await, 50)
        .await
    }
}

pub fn send_message_cron_job(state: AppState) -> JobHandler {
    Box::pin(async move {
        MessageChannel {
            id: "user_id".to_string(),
        }
        .send(
            state.get_redis_notification().await,
            Message {
                message: "hello, world".to_string(),
            },
        )
        .await
    })
}
