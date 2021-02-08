use bigdecimal::{BigDecimal, FromPrimitive};
use std::cmp::{max, min, Ordering};
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
    pub qty: Qty,
    pub price: BigDecimal,
    batches_out: u16,
    cleared: bool,
}

impl Order {
    pub(crate) fn new(price: BigDecimal, qty: Qty) -> Order {
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
        //TODO: try another sort
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
    let mut demand = orders_to_curve_segments(&bids);
    // supply curve
    let mut supply = orders_to_curve_segments(&asks);

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

    let mut max_q = 0;

    for order in orders {
        max_q += order.qty;
        segments.insert(order.price.clone(), max_q);
    }

    let mut segments: Vec<Segment> = segments
        .into_iter()
        .map(|(p, q)| Segment { price: p, q_max: q })
        .collect();

    segments.sort_unstable_by(|s1, s2| s1.q_max.cmp(&s2.q_max));

    segments
}

/**
* params: sorted curve's segmetns
* returns: p*, q*
*/
fn intersect_demand_supply(demand: &[Segment], supply: &[Segment]) -> Option<(BigDecimal, Qty)> {
    let mut idx_demand: usize = 0;
    let mut idx_supply: usize = 0;

    let mut idx_next_demand: usize = 0;
    let mut idx_next_supply: usize = 0;

    let size_demand: usize = demand.len();
    let size_supply: usize = supply.len();

    // no trades: no orders for side
    if demand.is_empty() || supply.is_empty() {
        return None;
    }

    // no trades: highest bid is lower than the lowes ask
    if demand[idx_demand].price < supply[idx_supply].price {
        return None;
    }

    let mut q_star: Qty = 0;

    while idx_next_demand < size_demand && idx_next_supply < size_supply {
        let seg_demand = &demand[idx_next_demand];
        let seg_supply = &supply[idx_next_supply];

        //check price for this segments
        if seg_supply.price > seg_demand.price {
            break;
        } else {
            // before intersection
            idx_demand = idx_next_demand;
            idx_supply = idx_next_supply;
            // move along demand is it strictly shorter
            if seg_demand.q_max < seg_supply.q_max {
                idx_next_demand += 1;
            } else {
                // move along supply
                idx_next_supply += 1;
            }
        }

        // q_star keeps the right edge
        q_star = min(seg_demand.q_max, seg_supply.q_max);
    }

    Some((
        (&demand[idx_demand].price + &supply[idx_supply].price) / 2,
        q_star,
    ))
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use crate::auction::{
        calculate_batch, intersect_demand_supply, orders_to_curve_segments, BatchReport, Order,
        Segment,
    };
    use bigdecimal::{BigDecimal, FromPrimitive};
    use rand::Rng;
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
        assert_eq!(segments.len(), 3);

        assert_eq!(
            segments.get(0).unwrap().price,
            BigDecimal::from_str("111.69").unwrap()
        );
        assert_eq!(segments.get(0).unwrap().q_max, 10);

        assert_eq!(
            segments.get(1).unwrap().price,
            BigDecimal::from_str("111.00").unwrap()
        );
        assert_eq!(segments.get(1).unwrap().q_max, 11);

        assert_eq!(
            segments.get(2).unwrap().price,
            BigDecimal::from_str("110.97").unwrap()
        );
        assert_eq!(segments.get(2).unwrap().q_max, 14);
    }

    #[test]
    fn calculate_batch_with_trades_correctly() {
        let mut bids = vec![
            Order::new(BigDecimal::from_str("112").unwrap(), 2),
            Order::new(BigDecimal::from_str("111.76").unwrap(), 21),
            Order::new(BigDecimal::from_str("111.45").unwrap(), 200),
            Order::new(BigDecimal::from_str("111.35").unwrap(), 100),
        ];

        let mut asks = vec![
            Order::new(BigDecimal::from_str("110").unwrap(), 2),
            Order::new(BigDecimal::from_str("111.32").unwrap(), 21),
            Order::new(BigDecimal::from_str("111.45").unwrap(), 100),
            Order::new(BigDecimal::from_str("112.35").unwrap(), 100),
        ];

        if let BatchReport::Trade {
            price,
            qty,
            cleared_bids,
            cleared_asks,
        } = calculate_batch(&mut bids, &mut asks)
        {
            assert_eq!(price, BigDecimal::from_str("111.45").unwrap());
            assert_eq!(qty, 123);
        } else {
            panic!();
        }
    }
}

extern crate test;
use rand::Rng;
use std::str::FromStr;
use test::Bencher;

#[bench]
fn batch(b: &mut Bencher) {
    let mut rng = rand::thread_rng();

    let mut bids: Vec<Order> = vec![];
    let mut asks: Vec<Order> = vec![];

    // pretty heterogeneous data
    for _ in 0..125000 {
        let random_price: f32 = rng.gen_range(140.0..150.0);
        let random_qty: u32 = rng.gen_range(1..200);
        let order = Order::new(
            BigDecimal::from_f32(random_price).unwrap().round(3),
            random_qty,
        );
        bids.push(order.clone());
        asks.push(order.clone());
    }

    b.iter(|| calculate_batch(&mut bids, &mut asks))
}
