// astra_kafka/src/kafka_key.rs
use std::fmt::{self, Display};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaKey(String);

impl KafkaKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Tenta interpretar a key como UUID (útil quando a key é o id da entidade)
    pub fn to_uuid(&self) -> Result<Uuid, uuid::Error> {
        Uuid::parse_str(&self.0)
    }
}

impl Display for KafkaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for KafkaKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for KafkaKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<Uuid> for KafkaKey {
    fn from(value: Uuid) -> Self {
        Self::from_uuid(value)
    }
}

impl AsRef<str> for KafkaKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}