use std::time::Duration;

pub(crate) mod enumerator;
mod source;
mod split;

pub use enumerator::*;

pub use split::*;

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);
