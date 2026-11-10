use std::env;

fn required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Variável de ambiente {key} não definida"))
}

pub fn get_db_url() -> String { required("DATABASE_URL") }
pub fn get_aws_region() -> String { required("AWS_REGION") }
pub fn get_aws_s3_bucket_name() -> String { required("AWS_S3_BUCKET_NAME") }
pub fn get_azure_client_id() -> String { required("AZURE_CLIENT_ID") }
pub fn get_azure_client_secret() -> String { required("AZURE_CLIENT_SECRET") }
pub fn get_azure_tenant_id() -> String { required("AZURE_TENANT_ID") }
pub fn get_azure_vault_url() -> String { required("AZURE_VAULT_URL") }