#![feature(test)]

use crate::auction::{calculate_batch, Order};
use bigdecimal::{BigDecimal, FromPrimitive};
use rand::Rng;

pub mod auction;

fn main() {
    let mut rng = rand::thread_rng();

    let mut bids: Vec<Order> = vec![];
    let mut asks: Vec<Order> = vec![];

    for _ in 0..125000 {
        let random_price: f32 = rng.gen_range(100.0..150.0);
        let random_qty: u32 = rng.gen_range(1..200);
        let order = Order::new(BigDecimal::from_f32(random_price).unwrap(), random_qty);
        bids.push(order.clone());
        asks.push(order.clone());
    }

    calculate_batch(&mut bids, &mut asks);
}
