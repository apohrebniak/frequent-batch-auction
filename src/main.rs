#![feature(test)]

use crate::auction::{calculate_batch, Order};
use bigdecimal::{BigDecimal, FromPrimitive};
use rand::Rng;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;

pub mod auction;

const INTERVAL_MILLIS: u64 = 100;

type OrderBook = Arc<Mutex<BTreeSet<Order>>>;

enum CommandType {
    Add,
    Cancel,
}

enum OrderType {
    Buy,
    Sell,
}

struct Command {
    command_type: CommandType,
    order_type: OrderType,
    price: BigDecimal,
    qty: u32,
}

async fn run_auction() {
    println!("tick")
}

// assume it's always valid
fn parse_command(msg: String) -> Command {
    let mut split = msg.split(',');

    let command_type = match split.next().unwrap() {
        "ADD" => CommandType::Add,
        "CANCEL" => CommandType::Cancel,
        _ => panic!("unknown command"),
    };

    let order_type = match split.next().unwrap() {
        "BUY" => OrderType::Buy,
        "SELL" => OrderType::Sell,
        _ => panic!("unknown order type"),
    };

    // assume precision is 2 digits
    let price = BigDecimal::from_str(split.next().unwrap())
        .unwrap()
        .round(2);

    let qty = split.next().unwrap().trim().parse::<u32>().unwrap();

    Command {
        command_type,
        order_type,
        price,
        qty,
    }
}

fn process_message(msg: String) {
    let command = parse_command(msg);
}

async fn process_stream(tcp_stream: TcpStream) {
    let mut buff_reader = BufReader::new(tcp_stream);
    loop {
        let mut msg = String::new();
        buff_reader.read_line(&mut msg).await.unwrap(); // assume everything is OK
        process_message(msg);
    }
}

#[tokio::main]
async fn main() {
    // init order books
    let bid_order_book: OrderBook = Arc::new(Mutex::new(BTreeSet::new()));
    let ask_order_book: OrderBook = Arc::new(Mutex::new(BTreeSet::new()));

    // init channels
    // pipeline: socket -> channel -> order book

    //schedule periodic auction execution
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_millis(INTERVAL_MILLIS));
        loop {
            interval.tick().await;
            run_auction().await;
        }
    });

    //init task for order book updates
    // channel -> order book
    tokio::spawn(update_order_book());

    let mut tcp_listener = TcpListener::bind("0.0.0.0:7777").await.unwrap();

    loop {
        let (tcp_stream, _) = tcp_listener.accept().await.unwrap(); // assume everything is OK
        tokio::spawn(process_stream(tcp_stream));
    }
}
