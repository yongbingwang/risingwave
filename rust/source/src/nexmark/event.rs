use std::cmp::{max, min};
use std::collections::HashMap;

use chrono::NaiveDateTime;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use risingwave_common::types::{NaiveDateTimeWrapper, ScalarImpl};
use serde::{Deserialize, Serialize};

use crate::nexmark::config::{EventType, NEXMarkSourceConfig};

const MIN_STRING_LENGTH: usize = 3;

trait NEXMarkRng {
    fn gen_string(&mut self, max: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NEXMarkRng for SmallRng {
    fn gen_string(&mut self, max: usize) -> String {
        let len = self.gen_range(MIN_STRING_LENGTH..max);
        String::from(
            (0..len)
                .map(|_| {
                    if self.gen_range(0..13) == 0 {
                        String::from(" ")
                    } else {
                        ::std::char::from_u32('a' as u32 + self.gen_range(0..26))
                            .unwrap()
                            .to_string()
                    }
                })
                .collect::<Vec<String>>()
                .join("")
                .trim(),
        )
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf((*self).gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

type Id = usize;

/// The `NEXMark` Event, including `Person`, `Auction`, and `Bid`.
#[derive(Clone, Debug)]
pub struct Event {
    pub values: HashMap<String, ScalarImpl>,
    pub event_type: EventType,
}

impl Event {
    /// Creates a new event randomly.
    pub fn new(
        events_so_far: usize,
        nex: &NEXMarkSourceConfig,
        wall_clock_base_time: usize,
    ) -> (Event, usize) {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = nex.event_timestamp(nex.next_adjusted_event(events_so_far));
        let new_wall_clock_base_time = timestamp - nex.base_time + wall_clock_base_time;

        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);
        let mut rng = SmallRng::seed_from_u64(id as u64);
        if rem < nex.person_proportion {
            (
                Event {
                    values: PersonGenerator::generate(id, timestamp, &mut rng, nex),
                    event_type: EventType::Person,
                },
                new_wall_clock_base_time,
            )
        } else if rem < nex.person_proportion + nex.auction_proportion {
            (
                Event {
                    values: AuctionGenerator::generate(events_so_far, id, timestamp, &mut rng, nex),
                    event_type: EventType::Auction,
                },
                new_wall_clock_base_time,
            )
        } else {
            (
                Event {
                    values: BidGenerator::generate(id, timestamp, &mut rng, nex),
                    event_type: EventType::Bid,
                },
                new_wall_clock_base_time,
            )
        }
    }
}

/// Person represents a person submitting an item for auction and/or making a
/// bid on an auction.
pub struct PersonGenerator {}

impl PersonGenerator {
    /// Creates a new `Person` event.
    fn generate(
        id: usize,
        time: usize,
        rng: &mut SmallRng,
        nex: &NEXMarkSourceConfig,
    ) -> HashMap<String, ScalarImpl> {
        let p_id = Self::last_id(id, nex) + nex.first_person_id;
        let name = format!(
            "{} {}",
            nex.first_names.choose(rng).unwrap(),
            nex.last_names.choose(rng).unwrap(),
        );
        let email_address = format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5));
        let credit_card = (0..4)
            .map(|_| format!("{:04}", rng.gen_range(0..10000)))
            .collect::<Vec<String>>()
            .join(" ");
        let city = nex.us_cities.choose(rng).unwrap().clone();
        let state = nex.us_states.choose(rng).unwrap().clone();
        let p_date_time = time;
        HashMap::from([
            (String::from("p_id"), ScalarImpl::Int32(p_id as i32)),
            (String::from("name"), ScalarImpl::Utf8(name)),
            (
                String::from("email_address"),
                ScalarImpl::Utf8(email_address),
            ),
            (String::from("credit_card"), ScalarImpl::Utf8(credit_card)),
            (String::from("city"), ScalarImpl::Utf8(city)),
            (String::from("state"), ScalarImpl::Utf8(state)),
            (
                String::from("p_date_time"),
                milli_ts_to_naive_date_time(p_date_time),
            ),
        ])
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkSourceConfig) -> Id {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0..active + nex.person_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkSourceConfig) -> Id {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

/// Auction represents an item under auction.
pub struct AuctionGenerator {}

impl AuctionGenerator {
    fn generate(
        events_so_far: usize,
        id: usize,
        time: usize,
        rng: &mut SmallRng,
        nex: &NEXMarkSourceConfig,
    ) -> HashMap<String, ScalarImpl> {
        let initial_bid = rng.gen_price();
        let seller = if rng.gen_range(0..nex.hot_seller_ratio) > 0 {
            (PersonGenerator::last_id(id, nex) / nex.hot_seller_ratio_2) * nex.hot_seller_ratio_2
        } else {
            PersonGenerator::next_id(id, rng, nex)
        };
        let a_id = Self::last_id(id, nex) + nex.first_auction_id;
        let item_name = rng.gen_string(20);
        let description = rng.gen_string(100);
        let reserve = initial_bid + rng.gen_price();
        let a_date_time = time;
        let expires = time + Self::next_length(events_so_far, rng, time, nex);
        let seller = seller + nex.first_person_id;
        let category = nex.first_category_id + rng.gen_range(0..nex.num_categories);

        HashMap::from([
            (String::from("a_id"), ScalarImpl::Int32(a_id as i32)),
            (String::from("item_name"), ScalarImpl::Utf8(item_name)),
            (String::from("description"), ScalarImpl::Utf8(description)),
            (
                String::from("initial_bid"),
                ScalarImpl::Int32(initial_bid as i32),
            ),
            (String::from("reserve"), ScalarImpl::Int32(reserve as i32)),
            (
                String::from("a_date_time"),
                milli_ts_to_naive_date_time(a_date_time),
            ),
            (
                String::from("expires"),
                milli_ts_to_naive_date_time(expires),
            ),
            (String::from("seller"), ScalarImpl::Int32(seller as i32)),
            (String::from("category"), ScalarImpl::Int32(category as i32)),
        ])
    }

    fn next_id(id: usize, rng: &mut SmallRng, nex: &NEXMarkSourceConfig) -> Id {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction + rng.gen_range(0..max_auction - min_auction + 1 + nex.auction_id_lead)
    }

    fn last_id(id: usize, nex: &NEXMarkSourceConfig) -> Id {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    fn next_length(
        events_so_far: usize,
        rng: &mut SmallRng,
        time: usize,
        nex: &NEXMarkSourceConfig,
    ) -> usize {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp(current_event + events_for_auctions);

        let horizon = future_auction - time;
        1 + rng.gen_range(0..max(horizon * 2, 1))
    }
}

/// Bid represents a bid for an item under auction.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize, Debug, Hash)]
pub struct BidGenerator {}

impl BidGenerator {
    fn generate(
        id: usize,
        time: usize,
        rng: &mut SmallRng,
        nex: &NEXMarkSourceConfig,
    ) -> HashMap<String, ScalarImpl> {
        let auction = if 0 < rng.gen_range(0..nex.hot_auction_ratio) {
            (AuctionGenerator::last_id(id, nex) / nex.hot_auction_ratio_2) * nex.hot_auction_ratio_2
        } else {
            AuctionGenerator::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0..nex.hot_bidder_ratio) {
            (PersonGenerator::last_id(id, nex) / nex.hot_bidder_ratio_2) * nex.hot_bidder_ratio_2
                + 1
        } else {
            PersonGenerator::next_id(id, rng, nex)
        };
        let auction = auction + nex.first_auction_id;
        let bidder = bidder + nex.first_person_id;
        let price = rng.gen_price();
        let b_date_time = time;
        HashMap::from([
            (String::from("auction"), ScalarImpl::Int32(auction as i32)),
            (String::from("bidder"), ScalarImpl::Int32(bidder as i32)),
            (String::from("price"), ScalarImpl::Int32(price as i32)),
            (
                String::from("b_date_time"),
                milli_ts_to_naive_date_time(b_date_time),
            ),
        ])
    }
}

fn milli_ts_to_naive_date_time(milli_ts: usize) -> ScalarImpl {
    ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper(NaiveDateTime::from_timestamp(
        milli_ts as i64 / 1000,
        (milli_ts % (1000_usize)) as u32 * 1000000,
    )))
}
