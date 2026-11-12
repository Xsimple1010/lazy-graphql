use std::fmt::Display;

use sea_orm::DbErr;

#[derive(Debug)]
pub enum ServiceError {
    Validation(String),
    Database(DbErr),
}

impl From<DbErr> for ServiceError {
    fn from(value: DbErr) -> Self {
        Self::Database(value)
    }
}

impl From<ServiceError> for String {
    fn from(value: ServiceError) -> Self {
        value.to_string()
    }
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(e) => write!(f, "{e}"),
            Self::Database(e) => write!(f, "erro de banco de dados: {e}"),
        }
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Validation(_) => None,
            Self::Database(e) => Some(e),
        }
    }
}
