use anyhow::Result;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::sts::AssumeRoleProvider;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use risingwave_common::error::{ErrorCode, RwError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]

pub struct AwsAssumeRole {
    pub(crate) arn: String,
    pub(crate) external_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AwsConfigInfo {
    pub(crate) stream_name: String,
    pub(crate) region: Option<String>,
    pub(crate) credentials: Option<AwsCredentials>,
    pub(crate) assume_role: Option<AwsAssumeRole>,
    pub(crate) endpoint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl AwsConfigInfo {
    pub async fn load(&self) -> Result<aws_config::Config> {
        let region = self
            .region
            .as_ref()
            .ok_or_else(|| anyhow::Error::msg("region should be provided"))?;
        let region = Some(Region::new(region.clone()));

        let mut credentials_provider = match &self.credentials {
            Some(AwsCredentials {
                access_key_id,
                secret_access_key,
                session_token,
            }) => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
                access_key_id,
                secret_access_key,
                session_token.clone(),
            )),
            None => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .region(region.clone())
                    .build()
                    .await,
            ),
        };

        if let Some(AwsAssumeRole { arn, external_id }) = &self.assume_role {
            let mut role = AssumeRoleProvider::builder(arn).session_name("RisingWave");
            if let Some(region) = &region {
                role = role.region(region.clone());
            }
            if let Some(external_id) = external_id {
                role = role.external_id(external_id);
            }
            credentials_provider = SharedCredentialsProvider::new(role.build(credentials_provider));
        }

        let config_loader = aws_config::from_env()
            .region(region)
            .credentials_provider(credentials_provider);
        Ok(config_loader.load().await)
    }

    #[warn(clippy::too_many_arguments)]
    pub fn new(
        stream_name: String,
        region: Option<String>,
        endpoint: Option<String>,
        credentials_access_key: Option<String>,
        credential_secret_key: Option<String>,
        credential_session_token: Option<String>,
        assume_role_arn: Option<String>,
        assume_role_external_id: Option<String>,
    ) -> Result<Self, RwError> {
        let mut credentials: Option<AwsCredentials> = None;
        let mut assume_role: Option<AwsAssumeRole> = None;
        if credentials_access_key.is_some() ^ credential_secret_key.is_some() {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "Both AWS access key and AWS secret key should be provided.".to_string(),
            )));
        } else if let (Some(access_key), Some(secret_key)) =
            (credentials_access_key, credential_secret_key)
        {
            credentials = Some(AwsCredentials {
                access_key_id: access_key,
                secret_access_key: secret_key,
                session_token: credential_session_token,
            });
        }

        if let Some(arn) = assume_role_arn {
            assume_role = Some(AwsAssumeRole {
                arn,
                external_id: assume_role_external_id,
            });
        }
        Ok(Self {
            stream_name,
            region,
            credentials,
            assume_role,
            endpoint,
        })
    }
}
