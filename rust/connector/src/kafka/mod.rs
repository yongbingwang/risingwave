use std::time::Duration;

pub mod enumerator;
pub mod source;
pub mod split;

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);
