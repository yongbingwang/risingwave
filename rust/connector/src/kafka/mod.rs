use std::time::Duration;

mod enumerator;
mod source;
mod split;

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub broker_addr: Vec<String>,
}
