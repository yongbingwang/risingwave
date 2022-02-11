use std::cmp::Ordering;

use crate::error::Result;
use crate::util::range::CutType::{Closed, LowerInf, Open, UpperInf};

#[derive(PartialOrd, PartialEq, Ord, Eq, Copy, Clone)]
enum CutType {
    Open,
    Closed,
    LowerInf,
    UpperInf,
}

struct Cut<T: Ord + Eq + Default + 'static> {
    cut_type: CutType,
    value: T,
}

impl<T: Ord + Eq + Default + 'static> Ord for Cut<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T: Ord + Eq + Default + 'static> Eq for Cut<T> {}

impl<T: Ord + Eq + Default + 'static> PartialEq for Cut<T> {
    fn eq(&self, other: &Self) -> bool {
        return self.cut_type == other.cut_type && self.value == other.value;
    }
}

impl<T: Ord + Eq + Default + 'static> PartialOrd for Cut<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let res = match (&self.cut_type, &other.cut_type) {
            (Open, Open) | (Closed, Open) | (Open, Closed) => match self.value.cmp(&other.value) {
                Ordering::Equal => Ordering::Less,
                rest => rest,
            },
            (Closed, Closed) => self.value.cmp(&other.value),
            (_, UpperInf) => Ordering::Less,
            (LowerInf, _) => Ordering::Less,
            (_, LowerInf) => Ordering::Greater,
            (UpperInf, _) => Ordering::Greater,
        };
        Some(res)
    }
}

pub struct Range<T: Ord + Eq + Default + 'static> {
    lower_endpoint: Cut<T>,
    upper_endpoint: Cut<T>,
}

impl<T: Ord + Eq + Default + 'static> Range<T> {
    fn new(lower_endpoint: Cut<T>, upper_endpoint: Cut<T>) -> Self {
        Self {
            lower_endpoint,
            upper_endpoint,
        }
    }

    pub fn open(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Open,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Open,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn closed(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Closed,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Closed,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn left_open(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Open,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Closed,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn left_closed(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Closed,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Open,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn right_open(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Closed,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Open,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn right_closed(left: T, right: T) -> Result<Self> {
        ensure!(left <= right);
        let lower_endpoint = Cut {
            cut_type: Open,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: Closed,
            value: right,
        };
        Ok(Range::new(lower_endpoint, upper_endpoint))
    }

    pub fn left_open_right_inf(left: T) -> Self {
        let lower_endpoint = Cut {
            cut_type: Open,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: UpperInf,
            value: T::default(),
        };
        Range::new(lower_endpoint, upper_endpoint)
    }

    pub fn left_closed_right_inf(left: T) -> Self {
        let lower_endpoint = Cut {
            cut_type: Closed,
            value: left,
        };
        let upper_endpoint = Cut {
            cut_type: UpperInf,
            value: T::default(),
        };
        Range::new(lower_endpoint, upper_endpoint)
    }

    pub fn right_open_left_inf(right: T) -> Self {
        let lower_endpoint = Cut {
            cut_type: LowerInf,
            value: T::default(),
        };
        let upper_endpoint = Cut {
            cut_type: Open,
            value: right,
        };
        Range::new(lower_endpoint, upper_endpoint)
    }

    pub fn right_closed_left_inf(right: T) -> Self {
        let lower_endpoint = Cut {
            cut_type: LowerInf,
            value: T::default(),
        };
        let upper_endpoint = Cut {
            cut_type: Open,
            value: right,
        };
        Range::new(lower_endpoint, upper_endpoint)
    }

    pub fn inf() -> Self {
        let lower_endpoint = Cut {
            cut_type: LowerInf,
            value: T::default(),
        };
        let upper_endpoint = Cut {
            cut_type: UpperInf,
            value: T::default(),
        };
        Range::new(lower_endpoint, upper_endpoint)
    }

    pub fn contains(&self, value: &T) -> bool {
        let cmp_res = match self.lower_endpoint.cut_type {
            Closed => self.lower_endpoint.value <= *value,
            Open => self.lower_endpoint.value < *value,
            LowerInf => true,
            _ => panic!("Left endpoint of a range cannot be UpperInf"),
        };
        if !cmp_res {
            return false;
        }
        let cmp_res = match self.upper_endpoint.cut_type {
            Closed => *value <= self.upper_endpoint.value,
            Open => *value < self.upper_endpoint.value,
            UpperInf => true,
            _ => panic!("Right endpoint of a range cannot be LowerInf"),
        };
        cmp_res
    }

    pub fn intersect(&self, other: &Range<T>) -> bool {
        let res1 = self.upper_endpoint.cmp(&other.lower_endpoint);
        let res2 = self.lower_endpoint.cmp(&other.upper_endpoint);
        match (res1, res2) {
            (Ordering::Less, Ordering::Greater) => false,
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::util::range::CutType::{Closed, Open};
    use crate::util::range::{Cut, Range};

    #[test]
    fn test_cut() {
        let cut1 = Cut {
            cut_type: Open,
            value: 1,
        };
        let cut2 = Cut {
            cut_type: Closed,
            value: 1,
        };
        assert!(cut1 < cut2);
    }

    #[test]
    fn test_range() {
        let r1 = Range::left_open(0, 1).unwrap();
        assert!(!r1.contains(&0));
        assert!(r1.contains(&1));
        assert!(!r1.contains(&2));
        let r2 = Range::right_open("abc", "cde").unwrap();
        assert!(r2.contains(&"abd"));
        assert!(!r2.contains(&"cde"));
        assert!(!r2.contains(&"ab"));
        let r3 = Range::<i32>::inf();
        assert!(r3.contains(&i32::MAX));
        assert!(r3.contains(&i32::MAX));
        let r4 = Range::left_open_right_inf("abc");
        assert!(r4.contains(&"zzzzzz"));
        assert!(!r4.contains(&"ab"));
        let r5 = Range::right_open(Decimal::new(10000, 3), Decimal::new(10000, 2)).unwrap();
        assert!(r5.contains(&Decimal::new(99999999999, 9)));
        assert!(Range::open(1, 0).is_err());
        assert!(Range::closed(1, 1).is_ok());
    }
}
