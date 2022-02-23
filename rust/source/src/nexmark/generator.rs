use std::sync::Arc;
use std::thread::sleep;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

use risingwave_common::error::Result;
use risingwave_common::types::Datum;

use crate::event::Event;
use crate::nexmark::config::NEXMarkSourceConfig;
use crate::SourceColumnDesc;

#[derive(Clone, Debug)]
pub struct NEXMarkEventGenerator {
    pub events_so_far: usize,
    pub config: Arc<NEXMarkSourceConfig>,
    pub wall_clock_base_time: usize,
    pub reader_id: usize,
}

impl NEXMarkEventGenerator {
    pub async fn next(&mut self, columns: &[SourceColumnDesc]) -> Result<Vec<Vec<Datum>>> {
        let mut res: Vec<Vec<Datum>> = vec![];
        let mut num_event = 0;

        // Get unix timestamp in milliseconds
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        while num_event < self.config.max_chunk_size {
            let (mut event, new_wall_clock_base_time) =
                Event::new(self.events_so_far, &self.config, self.wall_clock_base_time);

            // When the generated timestamp is larger then current timestamp, if its the first
            // event, sleep and continue. Otherwise, directly return.
            if current_timestamp < new_wall_clock_base_time as u64 {
                if num_event == 0 {
                    sleep(time::Duration::from_millis(
                        new_wall_clock_base_time as u64 - current_timestamp,
                    ));
                } else {
                    break;
                }
            }
            self.wall_clock_base_time = new_wall_clock_base_time;
            self.events_so_far += 1;

            if event.event_type != self.config.table_type
                || self.events_so_far % 4 != self.reader_id
            {
                continue;
            }

            num_event += 1;

            res.push(
                columns
                    .iter()
                    .map(|column| {
                        if column.skip_parse {
                            None
                        } else {
                            let tmp = event.values.remove(&column.name)?;
                            Some(tmp)
                        }
                    })
                    .collect::<Vec<Datum>>(),
            );
        }
        Ok(res)
    }
}
