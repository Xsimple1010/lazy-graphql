use crate::kafka_common::kafka_key::KafkaKey;
use crate::kafka_common::setup::kafka_setup;
use crate::service_state::ServiceState;
use async_graphql::SubscriptionType;
use async_trait::async_trait;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;
use std::time::Duration;

#[async_trait]
pub trait NewServiceKafkaTrait<P: Sized + DeserializeOwned + Sync + Send + Serialize>:
    KafkaConsumerTrait<P> + KafkaProducerTrait<P>
{
    const TOPIC: &'static str;

    async fn start_consumer<
        Query: async_graphql::ObjectType + 'static,
        Mutation: async_graphql::ObjectType + 'static,
        Subscription: SubscriptionType + 'static,
        ActorClaims: DeserializeOwned + Send + Sync + 'static,
        S: ServiceState<Query, Mutation, Subscription, ActorClaims> + Send + Sync + 'static,
        F: Fn(KafkaKey, P, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    >(
        state: S,
        handler: F,
    ) where
        Self: Send,
    {
        tokio::task::spawn(async move {
            let consumer_group = state.get_app_name();

            Self::start_continuous_consumer(
                move |key, payload| {
                    let state = state.clone();
                    handler(key, payload, state)
                },
                consumer_group,
                Self::TOPIC,
            )
            .await;
        });
    }

    async fn send_message<'a>(
        producer: &FutureProducer,
        key: &KafkaKey,
        msg: &P,
    ) -> Result<(), &'a str> {
        Self::send(producer, key, msg, Self::TOPIC).await
    }
}

#[async_trait::async_trait]
pub trait KafkaConsumerTrait<R: Sized + DeserializeOwned + Sync + Send> {
    fn setup<'a>(consumer_group: &str, topic: &'static str) -> Result<StreamConsumer, &'a str> {
        Self::ensure_topic_exists(topic);

        let mut config = kafka_setup();
        config
            .set("group.id", consumer_group)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true");

        let consumer: StreamConsumer = config.create().map_err(|e| {
            println!("❌ Failed to create Kafka consumer: {e:?}");
            "Não conseguimos recuperar o consumer do tópico"
        })?;

        consumer.subscribe(&[topic]).map_err(|e| {
            println!("❌ Failed to subscribe to topic {}: {:?}", topic, e);
            "Failed to subscribe to topic"
        })?;

        Ok(consumer)
    }

    fn ensure_topic_exists(topic: &'static str) {
        use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
        use rdkafka::client::DefaultClientContext;

        let bootstrap_servers =
            std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or("kafka:9092".to_string());

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .create()
            .expect("Falha ao criar AdminClient");

        let topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let results = admin
                    .create_topics(&[topic], &AdminOptions::new())
                    .await
                    .expect("Falha ao criar tópico");

                for result in results {
                    match result {
                        Ok(name) => println!("✅ Tópico '{name}' criado"),
                        Err((name, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
                            println!("ℹ️ Tópico '{name}' já existe")
                        }
                        Err((name, e)) => println!("⚠️ Erro ao criar tópico '{name}': {e:?}"),
                    }
                }
            })
        });
    }

    async fn get_message_content<'a>(
        consumer: &StreamConsumer,
    ) -> Result<(KafkaKey, String), &'a str> {
        let message = consumer.recv().await.map_err(|e| {
            println!("❌ Failed to receive Kafka message: {e:?}");
            "Failed to receive Kafka message"
        })?;

        let key = message
            .key()
            .map(|k| String::from_utf8_lossy(k).to_string())
            .ok_or("Não foi possível recuperar key do kafka")?;

        let payload = message
            .payload()
            .map(|p| String::from_utf8_lossy(p).to_string())
            .ok_or("Não foi possível recuperar o payload do kafka")?;

        Ok((key.into(), payload))
    }

    async fn start_continuous_consumer<F, Fut>(
        handler: F,
        consumer_group: &str,
        topic: &'static str,
    ) where
        F: Fn(KafkaKey, R) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let consumer = match Self::setup(&format!("{}:{}", consumer_group, topic), topic) {
            Ok(con) => con,
            Err(e) => {
                println!("StreamConsumer Err: {e}");
                return;
            }
        };

        loop {
            match Self::get_message_content(&consumer).await {
                Ok((key, payload)) => {
                    let payload = match serde_json::from_str::<R>(&payload) {
                        Ok(payload) => payload,
                        Err(e) => {
                            println!("❌ Failed to parse payload: {e:?}");
                            continue;
                        }
                    };

                    println!("✅ Processing message for ID: {key}");
                    handler(key, payload).await;
                }
                Err(e) => println!("⚠️ Consumer error (continuing): {e}"),
            }
        }
    }
}

#[async_trait::async_trait]
pub trait KafkaProducerTrait<M: ?Sized + Serialize + Sync> {
    async fn send<'a>(
        producer: &FutureProducer,
        key: &KafkaKey,
        msg: &M,
        topic: &'static str,
    ) -> Result<(), &'a str> {
        let msg = &serde_json::to_string(msg)
            .map_err(|_| "Não conseguimos atualizar os seus dados, por favor tente novamente.")?;

        let key = key.to_string();
        let record = FutureRecord::to(topic).payload(msg).key(&key);

        match producer
            .send(record, Timeout::After(Duration::from_secs(1)))
            .await
        {
            Err(e) => {
                println!("kafka error: {e:?}");
                Err(
                    "Não notifica nossos servidores sobre o seu pedido de atualização, por favor tente novamente",
                )
            }
            Ok(report) => {
                println!("kafka suss: {report:?}");
                Ok(())
            }
        }
    }
}
