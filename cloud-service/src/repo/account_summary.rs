use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use conditional_trait_gen::{trait_gen, when};
use golem_common::model::AccountId;
use sqlx::{Database, Pool};

use crate::model::AccountSummary;
use crate::repo::RepoError;

#[async_trait]
pub trait AccountSummaryRepo {
    async fn get(&self, skip: i32, limit: i32) -> Result<Vec<AccountSummary>, RepoError>;
    async fn count(&self) -> Result<u64, RepoError>;
}

#[derive(sqlx::FromRow)]
pub struct AccountSummaryRecord {
    id: String,
    name: String,
    email: String,
    components_count: i64,
    workers_count: i64,
    created_at: DateTime<Utc>,
}

impl From<AccountSummaryRecord> for AccountSummary {
    fn from(value: AccountSummaryRecord) -> Self {
        AccountSummary {
            id: AccountId { value: value.id },
            name: value.name,
            email: value.email,
            component_count: value.components_count,
            worker_count: value.workers_count,
            created_at: value.created_at,
        }
    }
}

pub struct DbAccountSummaryRepo<DB: Database> {
    db_pool: Arc<Pool<DB>>,
}

impl<DB: Database> DbAccountSummaryRepo<DB> {
    pub fn new(db_pool: Arc<Pool<DB>>) -> Self {
        Self { db_pool }
    }
}

#[trait_gen(sqlx::Postgres -> sqlx::Postgres, sqlx::Sqlite)]
#[async_trait]
impl AccountSummaryRepo for DbAccountSummaryRepo<sqlx::Postgres> {
    #[when(sqlx::Postgres -> get)]
    async fn get_postgres(&self, skip: i32, limit: i32) -> Result<Vec<AccountSummary>, RepoError> {
        let result = sqlx::query_as::<_, AccountSummaryRecord>(
          "
          SELECT a.id, a.name, a.email, COALESCE(ac.counter, 0::bigint) AS components_count, COALESCE(aw.counter, 0::bigint) AS workers_count, t.created_at::timestamptz
          FROM accounts a  
          JOIN (SELECT min(t.created_at) AS created_at, t.account_id FROM tokens t GROUP BY t.account_id) t ON t.account_id = a.id
          LEFT JOIN project_account pa ON pa.owner_account_id = a.id 
          LEFT JOIN account_components ac ON ac.account_id = a.id
          LEFT JOIN account_workers aw ON aw.account_id = a.id
          GROUP BY a.id, a.name, a.email, t.created_at, ac.counter, aw.counter, a.deleted
          ORDER BY t.created_at DESC, a.id DESC
         LIMIT $1
         OFFSET $2
          ",
      )
      .bind(limit)
      .bind(skip)
      .fetch_all(self.db_pool.as_ref())
      .await?;

        Ok(result.into_iter().map(|r| r.into()).collect())
    }

    #[when(sqlx::Sqlite -> get)]
    async fn get_sqlite(&self, skip: i32, limit: i32) -> Result<Vec<AccountSummary>, RepoError> {
        let result = sqlx::query_as::<_, AccountSummaryRecord>(
            "
          SELECT a.id, a.name, a.email, CAST(IFNULL(ac.counter, 0) AS bigint) AS components_count, CAST(IFNULL(aw.counter, 0) AS bigint) AS workers_count, t.created_at
          FROM accounts a
          JOIN (SELECT min(t.created_at) AS created_at, t.account_id FROM tokens t GROUP BY t.account_id) t ON t.account_id = a.id
          LEFT JOIN project_account pa ON pa.owner_account_id = a.id
          LEFT JOIN account_components ac ON ac.account_id = a.id
          LEFT JOIN account_workers aw ON aw.account_id = a.id
          GROUP BY a.id, a.name, a.email, t.created_at, ac.counter, aw.counter, a.deleted
          ORDER BY t.created_at DESC, a.id DESC
         LIMIT $1
         OFFSET $2
          ",
        )
            .bind(limit)
            .bind(skip)
            .fetch_all(self.db_pool.as_ref())
            .await?;

        Ok(result.into_iter().map(|r| r.into()).collect())
    }

    async fn count(&self) -> Result<u64, RepoError> {
        let result = sqlx::query_as::<_, (i64,)>("SELECT count(*) FROM accounts")
            .fetch_one(self.db_pool.as_ref())
            .await?;
        Ok(result.0 as u64)
    }
}
