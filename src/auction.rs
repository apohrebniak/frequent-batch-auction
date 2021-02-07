use bigdecimal::BigDecimal;
use std::cmp::Ordering;
use std::collections::HashMap;

type Qty = u32;

pub enum BatchReport {
    NoTrade,
    Trade {
        price: BigDecimal,
        qty: Qty,
        cleared_bids: Vec<Order>,
        cleared_asks: Vec<Order>,
    },
}

#[derive(Clone)]
pub struct Order {
    qty: Qty,
    price: BigDecimal,
    batches_out: u16,
    cleared: bool,
}

impl Order {
    fn new(price: BigDecimal, qty: Qty) -> Order {
        Order {
            qty,
            price,
            batches_out: 0,
            cleared: false,
        }
    }
}

struct Segment {
    price: BigDecimal,
    q_max: Qty,
}

pub fn calculate_batch(bids: &mut Vec<Order>, asks: &mut Vec<Order>) -> BatchReport {
    // sort bids. price high -> low, batches_out desc
    bids.sort_unstable_by(|order, other| {
        match order.price.cmp(&other.price) {
            Ordering::Equal => order.batches_out.cmp(&other.batches_out).reverse(), // old orders have priority
            x => x.reverse(), // high price has priority
        }
    });
    // sort asks. price low -> high, batches_out desc
    asks.sort_unstable_by(|order, other| {
        match order.price.cmp(&other.price) {
            Ordering::Equal => order.batches_out.cmp(&other.batches_out).reverse(), // old orders have priority
            x => x, // low price has priority
        }
    });

    // demand curve
    let demand = orders_to_curve_segments(&bids);
    // supply curve
    let supply = orders_to_curve_segments(&asks);

    match intersect_demand_supply(&demand, &supply) {
        None => BatchReport::NoTrade,
        Some((p_star, q_star)) => {
            let cleared_bids =
                clear_orders(bids, &p_star, q_star, |bid_price, price| bid_price >= price);
            let cleared_asks =
                clear_orders(asks, &p_star, q_star, |ask_price, price| ask_price <= price);

            //remove cleared orders
            bids.retain(|order| !order.cleared);
            asks.retain(|order| !order.cleared);

            BatchReport::Trade {
                price: p_star.clone(),
                qty: q_star.clone(),
                cleared_bids,
                cleared_asks,
            }
        }
    }
}

fn clear_orders(
    orders: &mut Vec<Order>,
    p_star: &BigDecimal,
    mut q_star: Qty,
    price_predicate: fn(&BigDecimal, &BigDecimal) -> bool,
) -> Vec<Order> {
    let mut cleared: Vec<Order> = vec![];

    for order in orders {
        if price_predicate(&order.price, &p_star) {
            if order.qty <= q_star {
                // fully clear the order
                order.cleared = true;
                q_star -= order.qty;
                cleared.push(order.clone())
            } else {
                // partially clear the order
                let qty_diff = order.qty - q_star;
                order.qty = qty_diff;
                break;
            }
        } else {
            // no more orders with suitable price
            break;
        }
    }
    cleared
}

fn orders_to_curve_segments(orders: &[Order]) -> Vec<Segment> {
    let mut segments: HashMap<BigDecimal, Qty> = HashMap::new();

    for order in orders {
        if segments.contains_key(&order.price) {
            segments.insert(
                order.price.clone(),
                order.qty + segments.get(&order.price).unwrap(),
            );
        } else {
            segments.insert(order.price.clone(), order.qty);
        }
    }

    segments
        .into_iter()
        .map(|(p, q)| Segment { price: p, q_max: q })
        .collect()
}

/**
* params: sorted curve's segmetns
* returns: p*, q*
*/
fn intersect_demand_supply(demand: &[Segment], supply: &[Segment]) -> Option<(BigDecimal, Qty)> {
    let mut curr_demand_i: usize = 0;
    let mut curr_supply_i: usize = 0;

    let mut demand_i: usize = 0;
    let mut supply_i: usize = 0;

    let mut q_star: Qty = 0;

    let size_demand: usize = demand.len();
    let size_supply: usize = supply.len();

    // no trades: no orders for side
    if demand.is_empty() || supply.is_empty() {
        return None;
    }

    // no trades: highest bid is lower than the lowes ask
    if demand[demand_i].price < supply[supply_i].price {
        return None;
    }

    // find the max q* and segments it belongs to
    for q in 0_u32.. {
        let prev_bid_seg = &demand[demand_i];
        let prev_ask_seg = &supply[supply_i];

        if q > prev_bid_seg.q_max {
            // this q belongs to next bid segment
            curr_demand_i += 1;
        }

        if q > prev_ask_seg.q_max {
            // this q belongs to next ask segment
            curr_supply_i += 1;
        }

        // the end of the curve.
        if curr_demand_i >= size_demand || curr_supply_i >= size_supply {
            break;
        }

        let curr_bid_seg = &demand[curr_demand_i];
        let curr_ask_seg = &supply[curr_supply_i];

        // check current segments prices
        if curr_bid_seg.price < curr_ask_seg.price {
            // intersection ended on previous q
            break;
        } else {
            // no intersection or it's not the end. proceed with next segments
            demand_i = curr_demand_i;
            supply_i = curr_supply_i;
            q_star = q;
        }
    }

    let bid_intersection_seg = &demand[demand_i];
    let ask_intersection_seg = &supply[supply_i];

    // get midpoint price
    Some((
        (&bid_intersection_seg.price + &ask_intersection_seg.price) / 2,
        q_star,
    ))
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use crate::auction::{intersect_demand_supply, orders_to_curve_segments, Order, Segment};
    use bigdecimal::{BigDecimal, FromPrimitive};
    use std::str::FromStr;

    #[test]
    fn supply_demand_intersect_horizontally() {
        let bids = [
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(6.0).unwrap(),
                q_max: 3,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 6,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 8,
            },
        ];

        let asks = [
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 1,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 5,
            },
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 8,
            },
        ];

        if let Some((p_star, q_star)) = intersect_demand_supply(&bids, &asks) {
            assert_eq!(q_star, 5);
            assert_eq!(p_star, BigDecimal::from(5));
        }
    }

    #[test]
    fn supply_demand_intersect_vertically() {
        let bids = [
            Segment {
                price: BigDecimal::from_f32(8.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(6.0).unwrap(),
                q_max: 3,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 6,
            },
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 8,
            },
        ];

        let asks = [
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 1,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(6.0).unwrap(),
                q_max: 5,
            },
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 8,
            },
        ];

        if let Some((p_star, q_star)) = intersect_demand_supply(&bids, &asks) {
            assert_eq!(q_star, 4);
            assert_eq!(p_star, BigDecimal::from(4));
        }
    }

    #[test]
    fn supply_demand_intersect_in_one_dot() {
        let bids = [
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 3,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 6,
            },
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 9,
            },
        ];

        let asks = [
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 7,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
        ];

        if let Some((p_star, q_star)) = intersect_demand_supply(&bids, &asks) {
            assert_eq!(q_star, 6);
            assert_eq!(p_star, BigDecimal::from_f32(2.5).unwrap())
        }
    }

    #[test]
    fn supply_demand_intersect_not_enough_bids() {
        let bids = [
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 3,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 5,
            },
        ];

        let asks = [
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 7,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
        ];

        if let Some((p_star, q_star)) = intersect_demand_supply(&bids, &asks) {
            assert_eq!(q_star, 5);
            assert_eq!(p_star, BigDecimal::from_f32(2.5).unwrap());
        }
    }

    #[test]
    fn supply_demand_intersect_no_bids() {
        let bids = [];

        let asks = [
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 7,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 9,
            },
        ];

        assert!(intersect_demand_supply(&bids, &asks).is_none())
    }

    #[test]
    fn supply_demand_intersect_no_asks() {
        let bids = [
            Segment {
                price: BigDecimal::from_f32(7.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 3,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 5,
            },
        ];

        let asks = [];

        assert!(intersect_demand_supply(&bids, &asks).is_none())
    }

    #[test]
    fn supply_demand_intersect_no_intersect_possible() {
        let asks = [
            Segment {
                price: BigDecimal::from_f32(3.0).unwrap(),
                q_max: 2,
            },
            Segment {
                price: BigDecimal::from_f32(4.0).unwrap(),
                q_max: 4,
            },
            Segment {
                price: BigDecimal::from_f32(5.0).unwrap(),
                q_max: 5,
            },
        ];

        let bids = [
            Segment {
                price: BigDecimal::from_f32(2.0).unwrap(),
                q_max: 1,
            },
            Segment {
                price: BigDecimal::from_f32(1.0).unwrap(),
                q_max: 5,
            },
        ];

        assert!(intersect_demand_supply(&bids, &asks).is_none())
    }

    #[test]
    fn orders_to_curve_segments_converts_correctly() {
        let orders = vec![
            Order::new(BigDecimal::from_str("111.69").unwrap(), 3),
            Order::new(BigDecimal::from_str("111.69").unwrap(), 3),
            Order::new(BigDecimal::from_str("111.69").unwrap(), 4),
            Order::new(BigDecimal::from_str("111.00").unwrap(), 1),
            Order::new(BigDecimal::from_str("110.97").unwrap(), 2),
            Order::new(BigDecimal::from_str("110.97").unwrap(), 1),
        ];

        let mut segments = orders_to_curve_segments(&orders);
        segments.sort_by(|seg, other| seg.price.cmp(&other.price));
        assert_eq!(segments.len(), 3);

        assert_eq!(
            segments.get(2).unwrap().price,
            BigDecimal::from_str("111.69").unwrap()
        );
        assert_eq!(segments.get(2).unwrap().q_max, 10);

        assert_eq!(
            segments.get(1).unwrap().price,
            BigDecimal::from_str("111.00").unwrap()
        );
        assert_eq!(segments.get(1).unwrap().q_max, 1);

        assert_eq!(
            segments.get(0).unwrap().price,
            BigDecimal::from_str("110.97").unwrap()
        );
        assert_eq!(segments.get(0).unwrap().q_max, 3);
    }
}

extern crate test;
use test::Bencher;

#[bench]
fn foobar(b: &mut Bencher) {}
