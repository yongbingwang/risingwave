use std::collections::HashMap;
use std::f64::consts::PI;
use std::str::FromStr;

use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

/// Base time unit for the `NEXMark` benchmark.
pub const BASE_TIME: usize = 1_436_918_400_000;

fn split_string_arg(string: String) -> Vec<String> {
    string.split(',').map(String::from).collect::<Vec<String>>()
}

#[derive(PartialEq)]
enum RateShape {
    Square,
    Sine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventType {
    Person,
    Auction,
    Bid,
}

/// Nexmark Configuration
#[derive(Clone, Debug)]
pub struct NEXMarkSourceConfig {
    /// Maximum number of people to consider as active for placing auctions or
    /// bids.
    pub active_people: usize,
    /// Average number of auction which should be inflight at any time, per
    /// generator.
    pub in_flight_auctions: usize,
    /// Number of events in out-of-order groups.
    /// 1 implies no out-of-order events. 1000 implies every 1000 events per
    /// generator are emitted in pseudo-random order.
    pub out_of_order_group_size: usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio: usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio: usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio: usize,
    /// Event id of first event to be generated.
    /// Event ids are unique over all generators, and are used as a seed to
    /// generate each event's data.
    pub first_event_id: usize,
    /// First event number.
    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp.
    pub first_event_number: usize,
    /// Time for first event (ms since epoch).
    pub base_time: usize,
    /// Delay before changing the current inter-event delay.
    pub step_length: usize,
    /// Number of events per epoch.
    /// Derived from above. (Ie number of events to run through cycle for all
    /// interEventDelayUs entries).
    pub events_per_epoch: usize,
    /// True period of epoch in milliseconds. Derived from above. (Ie time to
    /// run through cycle for all interEventDelayUs entries).
    pub epoch_period: f32,
    /// Delay between events, in microseconds.
    /// If the array has more than one entry then the rate is changed every
    /// step_length, and wraps around.
    pub inter_event_delays: Vec<f32>,
    // Originally constants
    /// Auction categories.
    pub num_categories: usize,
    /// Use to calculate the next auction id.
    pub auction_id_lead: usize,
    /// Ratio of auctions for 'hot' sellers compared to all other people.
    pub hot_seller_ratio_2: usize,
    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    pub hot_auction_ratio_2: usize,
    /// Ratio of bids for 'hot' bidders compared to all other people.
    pub hot_bidder_ratio_2: usize,
    /// Person Proportion.
    pub person_proportion: usize,
    /// Auction Proportion.
    pub auction_proportion: usize,
    /// Bid Proportion.
    pub bid_proportion: usize,
    /// Proportion Denominator.
    pub proportion_denominator: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_auction_id: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_person_id: usize,
    /// We start the ids at specific values to help ensure the queries find a
    /// match even on small synthesized dataset sizes.
    pub first_category_id: usize,
    /// Use to calculate the next id.
    pub person_id_lead: usize,
    /// Use to calculate inter_event_delays for rate-shape sine.
    pub sine_approx_steps: usize,
    /// The collection of U.S. statees
    pub us_states: Vec<String>,
    /// The collection of U.S. cities.
    pub us_cities: Vec<String>,
    /// The collection of first names.
    pub first_names: Vec<String>,
    /// The collection of last names.
    pub last_names: Vec<String>,
    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    pub num_event_generators: usize,
    /// Maximum number of events generated for one call of Next
    pub max_chunk_size: usize,
    /// Person, Auction or Bid
    pub table_type: EventType,
}

impl NEXMarkSourceConfig {
    /// Creates the `NEXMark` configuration.
    pub fn new(config: &HashMap<String, String>) -> Self {
        let active_people = NEXMarkSourceConfig::get_as_or(config, "active-people", 1000);
        let in_flight_auctions = NEXMarkSourceConfig::get_as_or(config, "in-flight-auctions", 100);
        let out_of_order_group_size =
            NEXMarkSourceConfig::get_as_or(config, "out-of-order-group-size", 1);
        let hot_seller_ratio = NEXMarkSourceConfig::get_as_or(config, "hot-seller-ratio", 4);
        let hot_auction_ratio = NEXMarkSourceConfig::get_as_or(config, "hot-auction-ratio", 2);
        let hot_bidder_ratio = NEXMarkSourceConfig::get_as_or(config, "hot-bidder-ratio", 4);
        let first_event_id = NEXMarkSourceConfig::get_as_or(config, "first-event-id", 0);
        let first_event_number = NEXMarkSourceConfig::get_as_or(config, "first-event-number", 0);
        let num_categories = NEXMarkSourceConfig::get_as_or(config, "num-categories", 5);
        let auction_id_lead = NEXMarkSourceConfig::get_as_or(config, "auction-id-lead", 10);
        let hot_seller_ratio_2 = NEXMarkSourceConfig::get_as_or(config, "hot-seller-ratio-2", 100);
        let hot_auction_ratio_2 =
            NEXMarkSourceConfig::get_as_or(config, "hot-auction-ratio-2", 100);
        let hot_bidder_ratio_2 = NEXMarkSourceConfig::get_as_or(config, "hot-bidder-ratio-2", 100);
        let person_proportion = NEXMarkSourceConfig::get_as_or(config, "person-proportion", 1);
        let auction_proportion = NEXMarkSourceConfig::get_as_or(config, "auction-proportion", 3);
        let bid_proportion = NEXMarkSourceConfig::get_as_or(config, "bid-proportion", 46);
        let proportion_denominator = person_proportion + auction_proportion + bid_proportion;
        let first_auction_id = NEXMarkSourceConfig::get_as_or(config, "first-auction-id", 1000);
        let first_person_id = NEXMarkSourceConfig::get_as_or(config, "first-person-id", 1000);
        let first_category_id = NEXMarkSourceConfig::get_as_or(config, "first-category-id", 10);
        let person_id_lead = NEXMarkSourceConfig::get_as_or(config, "person-id-lead", 10);
        let sine_approx_steps = NEXMarkSourceConfig::get_as_or(config, "sine-approx-steps", 10);
        let base_time = NEXMarkSourceConfig::get_as_or(config, "base-time", BASE_TIME);
        let us_states = split_string_arg(NEXMarkSourceConfig::get_or(
            config,
            "us-states",
            "az,ca,id,or,wa,wy",
        ));
        let us_cities = split_string_arg(NEXMarkSourceConfig::get_or(
            config,
            "us-cities",
            "phoenix,los angeles,san francisco,boise,portland,bend,redmond,seattle,kent,cheyenne",
        ));
        let first_names = split_string_arg(NEXMarkSourceConfig::get_or(
            config,
            "first-names",
            "peter,paul,luke,john,saul,vicky,kate,julie,sarah,deiter,walter",
        ));
        let last_names = split_string_arg(NEXMarkSourceConfig::get_or(
            config,
            "last-names",
            "shultz,abrams,spencer,white,bartels,walton,smith,jones,noris",
        ));
        let rate_shape = if NEXMarkSourceConfig::get_or(config, "rate-shape", "sine") == "sine" {
            RateShape::Sine
        } else {
            RateShape::Square
        };
        let rate_period = NEXMarkSourceConfig::get_as_or(config, "rate-period", 600);
        let first_rate = NEXMarkSourceConfig::get_as_or(
            config,
            "first-event-rate",
            NEXMarkSourceConfig::get_as_or(config, "events-per-second", 10_000),
        );
        let next_rate = NEXMarkSourceConfig::get_as_or(config, "next-event-rate", first_rate);
        let us_per_unit = NEXMarkSourceConfig::get_as_or(config, "us-per-unit", 1_000_000); // Rate is in μs
        let generators = NEXMarkSourceConfig::get_as_or(config, "threads", 1) as f32;
        let max_chunk_size =
            NEXMarkSourceConfig::get_as_or(config, "max_chunk_size", DEFAULT_CHUNK_BUFFER_SIZE);

        let table_type = NEXMarkSourceConfig::get_or(config, "table_type", "Unknown");

        let table_type = match table_type.as_str() {
            "Person" => EventType::Person,
            "Auction" => EventType::Auction,
            "Bid" => EventType::Bid,
            _ => panic!(
                "Unknown NEXMark table_type: {}, must be 'Person', 'Auction' or 'Bid'.",
                table_type
            ),
        };

        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let rate_to_period = |r| (us_per_unit) as f32 / r as f32;
        if first_rate == next_rate {
            inter_event_delays.push(rate_to_period(first_rate) * generators);
        } else {
            match rate_shape {
                RateShape::Square => {
                    inter_event_delays.push(rate_to_period(first_rate) * generators);
                    inter_event_delays.push(rate_to_period(next_rate) * generators);
                }
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if rate_shape == RateShape::Square {
            2
        } else {
            sine_approx_steps
        };
        let step_length = (rate_period + n - 1) / n;
        let mut events_per_epoch = 0;
        let mut epoch_period = 0.0;
        if inter_event_delays.len() > 1 {
            for inter_event_delay in &inter_event_delays {
                let num_events_for_this_cycle =
                    (step_length * 1_000_000) as f32 / inter_event_delay;
                events_per_epoch += num_events_for_this_cycle.round() as usize;
                epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
            }
        }
        NEXMarkSourceConfig {
            active_people,
            in_flight_auctions,
            out_of_order_group_size,
            hot_seller_ratio,
            hot_auction_ratio,
            hot_bidder_ratio,
            first_event_id,
            first_event_number,
            base_time,
            step_length,
            events_per_epoch,
            epoch_period,
            inter_event_delays,
            // Originally constants
            num_categories,
            auction_id_lead,
            hot_seller_ratio_2,
            hot_auction_ratio_2,
            hot_bidder_ratio_2,
            person_proportion,
            auction_proportion,
            bid_proportion,
            proportion_denominator,
            first_auction_id,
            first_person_id,
            first_category_id,
            person_id_lead,
            sine_approx_steps,
            us_states,
            us_cities,
            first_names,
            last_names,
            num_event_generators: generators as usize,
            max_chunk_size,
            table_type,
        }
    }

    /// Returns a new event timestamp.
    pub fn event_timestamp(&self, event_number: usize) -> usize {
        if self.inter_event_delays.len() == 1 {
            return self.base_time
                + ((event_number as f32 * self.inter_event_delays[0]) / 1000.0).round() as usize;
        }

        let epoch = event_number / self.events_per_epoch;
        let mut event_i = event_number % self.events_per_epoch;
        let mut offset_in_epoch = 0.0;
        for inter_event_delay in &self.inter_event_delays {
            let num_events_for_this_cycle =
                (self.step_length * 1_000_000) as f32 / inter_event_delay;
            if self.out_of_order_group_size < num_events_for_this_cycle.round() as usize {
                let offset_in_cycle = event_i as f32 * inter_event_delay;
                return self.base_time
                    + (epoch as f32 * self.epoch_period
                        + offset_in_epoch
                        + offset_in_cycle / 1000.0)
                        .round() as usize;
            }
            event_i -= num_events_for_this_cycle.round() as usize;
            offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
        }
        0
    }

    /// Returns the next adjusted event.
    pub fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }

    /// Returns the value for the given key automatically parsed, or a default
    /// value if the key does not exist.
    fn get_as_or<T: FromStr>(config: &HashMap<String, String>, key: &str, default: T) -> T {
        NEXMarkSourceConfig::get_as(config, key).unwrap_or(default)
    }

    /// Returns the value for the given key automatically parsed if possible.
    fn get_as<T: FromStr>(config: &HashMap<String, String>, key: &str) -> Option<T> {
        config.get(key).and_then(|x| x.parse::<T>().ok())
    }

    /// Returns the value for the given key or a default value if the key does
    /// not exist.
    fn get_or(config: &HashMap<String, String>, key: &str, default: &str) -> String {
        config.get(key).map_or(String::from(default), |x| x.clone())
    }
}

// #[cfg(test)]
// mod tests {
//     use std::io::Result;

//     use super::*;

//     #[test]
//     fn test_config() -> Result<()> {
//         let config1 = Config::new();
//         assert_eq!(config1.get("hello"), None);

//         let mut config2 = Config::from(
//             vec!["--hello", "world", "--db", "424", "layoff"]
//                 .iter()
//                 .map(ToString::to_string),
//         )?;
//         config2.insert("net", "417".to_string());

//         assert_eq!(config2.get_or("0", "-1"), "layoff");
//         assert_eq!(config2.get_or("hello", "-1"), "world");
//         assert_eq!(config2.get_as_or("db", 424), 424);
//         assert_eq!(config2.get_as_or("net", 417), 417);

//         Ok(())
//     }

//     #[test]
//     fn test_nexmark_config() {
//         let mut config = Config::new();

//         config.insert("active_people", "1024".to_string());
//         let mut nexmark_cfg = NEXMarkSourceConfig::new(&config);
//         nexmark_cfg.event_timestamp(2048);
//         nexmark_cfg.next_adjusted_event(100000);

//         config.insert("rate-shape", "sine".to_string());
//         config.insert("next-event-rate", "512".to_string());
//         nexmark_cfg = NEXMarkSourceConfig::new(&config);
//         nexmark_cfg.event_timestamp(2048);
//         nexmark_cfg.next_adjusted_event(100000);

//         config.insert("rate-shape", "square".to_string());
//         nexmark_cfg = NEXMarkSourceConfig::new(&config);
//         nexmark_cfg.event_timestamp(2048);
//         nexmark_cfg.next_adjusted_event(100000);

//         config.insert("threads", "8".to_string());
//         nexmark_cfg = NEXMarkSourceConfig::new(&config);
//         nexmark_cfg.event_timestamp(2048);
//         nexmark_cfg.next_adjusted_event(100000);
//     }
// }
