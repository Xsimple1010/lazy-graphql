use rdkafka::ClientConfig;
use rdkafka::producer::FutureProducer;

pub fn kafka_setup() -> ClientConfig {
    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or("kafka:9092".to_string());

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &bootstrap_servers);

    config
}


pub fn create_producer() -> FutureProducer {
    kafka_setup()
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Falha ao criar o Kafka producer")
}