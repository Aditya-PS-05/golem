use crate::auth::AccountAuthorisation;
use crate::model::{Account, AccountData, Plan};
use crate::repo::account::{AccountRecord, AccountRepo};
use crate::service::plan::{PlanError, PlanService};
use async_trait::async_trait;
use cloud_common::model::PlanId;
use cloud_common::model::Role;
use golem_common::model::AccountId;
use golem_common::SafeDisplay;
use golem_service_base::repo::RepoError;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Account Not Found: {0}")]
    AccountNotFound(AccountId),
    #[error("Arg Validation error: {}", .0.join(", "))]
    ArgValidation(Vec<String>),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Internal error: {0}")]
    InternalRepoError(#[from] RepoError),
    #[error(transparent)]
    InternalPlanError(#[from] PlanError),
}

impl SafeDisplay for AccountError {
    fn to_safe_string(&self) -> String {
        match self {
            AccountError::Unauthorized(_) => self.to_string(),
            AccountError::AccountNotFound(_) => self.to_string(),
            AccountError::ArgValidation(_) => self.to_string(),
            AccountError::Internal(_) => self.to_string(),
            AccountError::InternalRepoError(inner) => inner.to_safe_string(),
            AccountError::InternalPlanError(inner) => inner.to_safe_string(),
        }
    }
}

impl From<String> for AccountError {
    fn from(error: String) -> Self {
        AccountError::Internal(error)
    }
}

#[async_trait]
pub trait AccountService {
    async fn create(
        &self,
        id: &AccountId,
        account: &AccountData,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError>;

    async fn update(
        &self,
        account_id: &AccountId,
        account: &AccountData,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError>;

    async fn get(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError>;

    async fn get_plan(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<Plan, AccountError>;

    async fn delete(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<(), AccountError>;
}

pub struct AccountServiceDefault {
    account_repo: Arc<dyn AccountRepo + Sync + Send>,
    plan_service: Arc<dyn PlanService + Sync + Send>,
}

impl AccountServiceDefault {
    pub fn new(
        account_repo: Arc<dyn AccountRepo + Sync + Send>,
        plan_service: Arc<dyn PlanService + Sync + Send>,
    ) -> Self {
        AccountServiceDefault {
            account_repo,
            plan_service,
        }
    }

    async fn get_default_plan_id(&self) -> Result<PlanId, AccountError> {
        let plan_id = self
            .plan_service
            .get_default_plan()
            .await
            .map(|plan| plan.plan_id)?;

        Ok(plan_id)
    }
}

#[async_trait]
impl AccountService for AccountServiceDefault {
    async fn create(
        &self,
        id: &AccountId,
        account: &AccountData,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError> {
        check_root(auth)?;
        let plan_id = self.get_default_plan_id().await?;
        info!("Creating account: {}", id);
        match self
            .account_repo
            .create(&AccountRecord {
                id: id.clone().value,
                name: account.name.clone(),
                email: account.email.clone(),
                plan_id: plan_id.0,
            })
            .await
        {
            Ok(Some(account_record)) => Ok(account_record.into()),
            Ok(None) => Err(format!("Duplicated account on fresh id: {}", id).into()),
            Err(err) => {
                error!("DB call failed. {}", err);
                Err(err.into())
            }
        }
    }

    async fn update(
        &self,
        id: &AccountId,
        account: &AccountData,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError> {
        check_authorized(id, auth)?;
        info!("Updating account: {}", id);
        let current_account = self.account_repo.get(&id.value).await?;
        let plan_id = match current_account {
            Some(current_account) => current_account.plan_id,
            None => self.get_default_plan_id().await?.0,
        };
        let result = self
            .account_repo
            .update(&AccountRecord {
                id: id.value.clone(),
                name: account.name.clone(),
                email: account.email.clone(),
                plan_id,
            })
            .await;
        match result {
            Ok(account_record) => Ok(account_record.into()),
            Err(err) => {
                error!("DB call failed. {}", err);
                Err(err.into())
            }
        }
    }

    async fn get(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<Account, AccountError> {
        check_authorized(account_id, auth)?;
        info!("Get account: {}", account_id);
        let result = self.account_repo.get(&account_id.value).await;
        match result {
            Ok(Some(account_record)) => Ok(account_record.into()),
            Ok(None) => Err(AccountError::AccountNotFound(account_id.clone())),
            Err(err) => {
                error!("DB call failed. {}", err);
                Err(err.into())
            }
        }
    }

    async fn get_plan(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<Plan, AccountError> {
        check_authorized(account_id, auth)?;
        let result = self.account_repo.get(&account_id.value).await;
        match result {
            Ok(Some(account_record)) => {
                match self.plan_service.get(&PlanId(account_record.plan_id)).await {
                    Ok(Some(plan)) => Ok(plan),
                    Ok(None) => Err(format!(
                        "Could not find plan with id: {}",
                        account_record.plan_id
                    )
                    .into()),
                    Err(err) => {
                        error!("DB call failed. {:?}", err);
                        Err(err.into())
                    }
                }
            }
            Ok(None) => Err(AccountError::AccountNotFound(account_id.clone())),
            Err(err) => {
                error!("DB call failed. {}", err);
                Err(err.into())
            }
        }
    }

    async fn delete(
        &self,
        account_id: &AccountId,
        auth: &AccountAuthorisation,
    ) -> Result<(), AccountError> {
        check_root(auth)?;
        if auth.token.account_id == *account_id {
            return Err(AccountError::ArgValidation(vec![
                "Cannot delete current account.".to_string(),
            ]));
        }
        let result = self.account_repo.delete(&account_id.value).await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("DB call failed. {}", err);
                Err(err.into())
            }
        }
    }
}

fn check_authorized(
    account_id: &AccountId,
    auth: &AccountAuthorisation,
) -> Result<(), AccountError> {
    if auth.has_account_or_role(account_id, &Role::Admin) {
        Ok(())
    } else {
        Err(AccountError::Unauthorized(
            "Access to another account.".to_string(),
        ))
    }
}

fn check_root(auth: &AccountAuthorisation) -> Result<(), AccountError> {
    if auth.has_role(&Role::Admin) {
        Ok(())
    } else {
        Err(AccountError::Unauthorized(
            "Admin role required.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::AccountAuthorisation;
    use crate::service::account::{check_authorized, check_root};
    use cloud_common::model::Role;
    use golem_common::model::AccountId;

    #[test]
    pub fn test_check_authorized() {
        let account_id = AccountId::from("1");
        let account_id2 = AccountId::from("2");

        let auth = AccountAuthorisation::new_test(&account_id, Role::all());
        assert!(check_authorized(&account_id, &auth).is_ok());
        assert!(check_authorized(&account_id2, &auth).is_ok());

        let auth = AccountAuthorisation::new_test(&account_id, vec![Role::ViewProject]);
        assert!(check_authorized(&account_id, &auth).is_ok());
        assert!(check_authorized(&account_id2, &auth).is_err());
    }

    #[test]
    pub fn test_check_root() {
        let account_id = AccountId::from("1");

        let auth = AccountAuthorisation::new_test(&account_id, Role::all());
        assert!(check_root(&auth).is_ok());

        let auth: AccountAuthorisation =
            AccountAuthorisation::new_test(&account_id, vec![Role::ViewProject]);
        assert!(check_root(&auth).is_err());
    }
}
