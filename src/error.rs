use std::fmt::Display;

#[derive(Debug)]
pub enum ServiceError {
    Validation(String),
}

impl From<ServiceError> for String {
    fn from(value: ServiceError) -> Self {
        match value {
            ServiceError::Validation(err) => err,
        }
    }
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(e) => write!(f, "{}", e),
        }
    }
}
