use crate::error::ServiceError;
use async_graphql::futures_util::Stream;
use dashmap::DashMap;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{AsyncCommands, Client, PushInfo, PushKind, RedisError, Value};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};
use tokio::time::interval;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

type Observables = Arc<DashMap<String, Sender<String>>>;

#[derive(Clone)]
pub struct RedisNotification {
    observables: Observables,
    pub connection_manager: ConnectionManager,
}

pub use redis::{RedisWrite, ToRedisArgs, ToSingleRedisArg};

impl RedisNotification {
    pub async fn new(redis_client: Client, default_chanel: &str) -> Result<Self, ServiceError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let config = ConnectionManagerConfig::new()
            .set_push_sender(tx)
            .set_automatic_resubscription();

        let mut connection_manager = ConnectionManager::new_with_config(redis_client, config)
            .await
            .map_err(|e| ServiceError::Validation(e.to_string()))?;

        connection_manager
            .psubscribe(default_chanel)
            .await
            .map_err(|e| ServiceError::Validation(e.to_string()))?;

        let observables: Observables = Arc::new(DashMap::new());
        Self::spawn_main_task(observables.clone(), rx).await;
        Self::spawn_clear_observables(observables.clone()).await;

        Ok(Self {
            observables,
            connection_manager,
        })
    }

    async fn spawn_main_task(
        mut observables: Observables,
        mut main_rx: UnboundedReceiver<PushInfo>,
    ) {
        tokio::spawn(async move {
            while let Some(mut msg) = main_rx.recv().await {
                match msg.kind {
                    PushKind::Message | PushKind::PMessage => {}
                    PushKind::Subscribe
                    | PushKind::PSubscribe
                    | PushKind::Unsubscribe
                    | PushKind::PUnsubscribe => continue,
                    _ => {
                        println!("Evento Redis ignorado: {:?}", msg.kind);
                        continue;
                    }
                }

                if msg.data.len() < 2 {
                    println!("Formato da message inválido: {:?}", msg);
                    continue;
                }

                let chanel = match Self::get_string_value(msg.data.get_mut(0)) {
                    Ok(chanel) => chanel,
                    Err(e) => {
                        println!("Erro ao obter o canal: {}", e);
                        continue;
                    }
                };

                let message = match Self::get_string_value(msg.data.get_mut(1)) {
                    Ok(message) => message,
                    Err(e) => {
                        println!("Erro ao obter a mensagem: {}", e);
                        continue;
                    }
                };

                Self::send_message(&mut observables, &chanel, message).await;
            }
        });
    }

    async fn send_message(observables: &mut Observables, channel: &String, message: String) {
        if let Some(tx) = observables.get(channel) {
            let _ = tx.send(message).await;
        }
    }

    fn get_string_value(data: Option<&mut Value>) -> Result<String, String> {
        if let Some(val) = data {
            return match val {
                Value::BulkString(e) => Ok(String::from_utf8_lossy(e).to_string()),
                _ => Err(format!("O valor recebido não é uma string: {:?}", val)),
            };
        }
        Err("Valor enviado não existe".to_string())
    }

    async fn spawn_clear_observables(observables: Observables) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(500));
            loop {
                ticker.tick().await;
                observables.retain(|_key, sender| !sender.is_closed());
            }
        });
    }

    async fn append_observable<T>(
        &self,
        channel: String,
        buffer_size: usize,
    ) -> Result<impl Stream<Item = T> + use<T>, String>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<String>(buffer_size);

        self.connection_manager
            .clone()
            .subscribe(channel.clone())
            .await
            .map_err(|e| e.to_string())?;

        self.observables.insert(channel, tx);

        let stream = ReceiverStream::new(rx).filter_map(|msg| serde_json::from_str::<T>(&msg).ok());

        Ok(stream)
    }

    async fn send_message_to_channel<
        K: ToSingleRedisArg + Send + Sync + Debug,
        E: ToSingleRedisArg + Send + Sync + Debug,
    >(
        &self,
        channel: K,
        message: E,
    ) {
        let e: Result<(), RedisError> = self
            .connection_manager
            .clone()
            .publish(channel, message)
            .await;

        if let Err(e) = e {
            println!("Erro ao enviar message para o Redis: {:?}", e);
        }
    }
}

pub trait NotificationChannel: Send + Sync + 'static {
    /// Tipo que trafega neste canal.
    type Payload: ToSingleRedisArg + Debug + DeserializeOwned + Send + Sync + 'static;

    fn channel_name(&self) -> String;

    fn send(
        &self,
        redis: RedisNotification,
        data: Self::Payload,
    ) -> impl Future<Output = ()> + Send {
        async move {
            tracing::debug!("send:: {}", self.channel_name());
            redis
                .send_message_to_channel(self.channel_name(), data)
                .await;
        }
    }

    fn subscribe(
        self,
        redis: RedisNotification,
        buffer: usize,
    ) -> impl Future<Output = Result<impl Stream<Item = Self::Payload> + Send, String>> + Send
    where
        Self: Sized,
    {
        async move {
            redis
                .append_observable::<Self::Payload>(self.channel_name(), buffer)
                .await
        }
    }
}
