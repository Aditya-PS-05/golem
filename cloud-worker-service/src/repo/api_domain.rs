use crate::model::{AccountApiDomain, ApiDomain, DomainRequest};
use async_trait::async_trait;
use cloud_common::auth::CloudNamespace;
use conditional_trait_gen::{trait_gen, when};
use golem_common::model::AccountId;
use golem_worker_service_base::repo::RepoError;
use sqlx::{Database, Pool};
use std::ops::Deref;
use std::sync::Arc;

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ApiDomainRecord {
    pub namespace: String,
    pub domain_name: String,
    pub name_servers: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl ApiDomainRecord {
    pub fn new(
        account_id: AccountId,
        domain: DomainRequest,
        name_servers: Vec<String>,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            namespace: CloudNamespace {
                account_id,
                project_id: domain.project_id,
            }
            .to_string(),
            domain_name: domain.domain_name,
            name_servers: name_servers.join(","),
            created_at,
        }
    }
}

impl TryFrom<ApiDomainRecord> for AccountApiDomain {
    type Error = String;

    fn try_from(value: ApiDomainRecord) -> Result<Self, Self::Error> {
        let namespace: CloudNamespace = value.namespace.try_into()?;

        let name_servers = if value.name_servers.is_empty() {
            vec![]
        } else {
            value
                .name_servers
                .split(',')
                .map(|s| s.to_string())
                .collect()
        };

        Ok(AccountApiDomain {
            account_id: namespace.account_id,
            domain: ApiDomain {
                project_id: namespace.project_id,
                domain_name: value.domain_name,
                name_servers,
                created_at: Some(value.created_at),
            },
        })
    }
}

impl TryFrom<ApiDomainRecord> for ApiDomain {
    type Error = String;

    fn try_from(value: ApiDomainRecord) -> Result<Self, Self::Error> {
        let value: AccountApiDomain = value.try_into()?;
        Ok(value.domain)
    }
}

#[async_trait]
pub trait ApiDomainRepo {
    async fn create_or_update(&self, record: &ApiDomainRecord) -> Result<(), RepoError>;

    async fn get(&self, domain_name: &str) -> Result<Option<ApiDomainRecord>, RepoError>;

    async fn delete(&self, domain_name: &str) -> Result<bool, RepoError>;

    async fn get_all(&self, namespace: &str) -> Result<Vec<ApiDomainRecord>, RepoError>;
}

pub struct DbApiDomainRepo<DB: Database> {
    db_pool: Arc<Pool<DB>>,
}

impl<DB: Database> DbApiDomainRepo<DB> {
    pub fn new(db_pool: Arc<Pool<DB>>) -> Self {
        Self { db_pool }
    }
}

#[trait_gen(sqlx::Postgres -> sqlx::Postgres, sqlx::Sqlite)]
#[async_trait]
impl ApiDomainRepo for DbApiDomainRepo<sqlx::Postgres> {
    async fn create_or_update(&self, record: &ApiDomainRecord) -> Result<(), RepoError> {
        sqlx::query(
            r#"
               INSERT INTO api_domains
                (namespace, domain_name, name_servers, created_at)
              VALUES
                ($1, $2, $3, $4)
              ON CONFLICT (namespace, domain_name) DO UPDATE
              SET name_servers = $3
            "#,
        )
        .bind(record.namespace.clone())
        .bind(record.domain_name.clone())
        .bind(record.name_servers.clone())
        .bind(record.created_at)
        .execute(self.db_pool.deref())
        .await?;

        Ok(())
    }

    #[when(sqlx::Sqlite -> get)]
    async fn get_sqlite(&self, domain_name: &str) -> Result<Option<ApiDomainRecord>, RepoError> {
        sqlx::query_as::<_, ApiDomainRecord>("SELECT namespace, domain_name, name_servers, created_at FROM api_domains WHERE domain_name = $1")
            .bind(domain_name)
            .fetch_optional(self.db_pool.deref())
            .await
            .map_err(|e| e.into())
    }

    #[when(sqlx::Postgres -> get)]
    async fn get_postgres(&self, domain_name: &str) -> Result<Option<ApiDomainRecord>, RepoError> {
        sqlx::query_as::<_, ApiDomainRecord>("SELECT namespace, domain_name, name_servers, created_at::timestamptz FROM api_domains WHERE domain_name = $1")
            .bind(domain_name)
            .fetch_optional(self.db_pool.deref())
            .await
            .map_err(|e| e.into())
    }

    async fn delete(&self, domain_name: &str) -> Result<bool, RepoError> {
        sqlx::query("DELETE FROM api_domains WHERE domain_name = $1")
            .bind(domain_name)
            .execute(self.db_pool.deref())
            .await?;
        Ok(true)
    }

    #[when(sqlx::Sqlite -> get_all)]
    async fn get_all_sqlite(&self, namespace: &str) -> Result<Vec<ApiDomainRecord>, RepoError> {
        sqlx::query_as::<_, ApiDomainRecord>("SELECT namespace, domain_name, name_servers, created_at FROM api_domains WHERE namespace = $1")
            .bind(namespace)
            .fetch_all(self.db_pool.deref())
            .await
            .map_err(|e| e.into())
    }

    #[when(sqlx::Postgres -> get_all)]
    async fn get_all_postgres(&self, namespace: &str) -> Result<Vec<ApiDomainRecord>, RepoError> {
        sqlx::query_as::<_, ApiDomainRecord>("SELECT namespace, domain_name, name_servers, created_at::timestamptz FROM api_domains WHERE namespace = $1")
            .bind(namespace)
            .fetch_all(self.db_pool.deref())
            .await
            .map_err(|e| e.into())
    }
}
