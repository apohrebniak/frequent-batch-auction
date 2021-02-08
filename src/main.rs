#![feature(test)]

use crate::auction::{calculate_batch, BatchReport, Order};
use bigdecimal::{BigDecimal, FromPrimitive};
use rand::Rng;
use std::cell::RefCell;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Duration;

pub mod auction;

const INTERVAL_MILLIS: u64 = 100;

struct DoubleSidedBook {
    bids: Mutex<Vec<Order>>,
    asks: Mutex<Vec<Order>>,
}

impl DoubleSidedBook {
    fn new() -> DoubleSidedBook {
        DoubleSidedBook {
            bids: Mutex::new(vec![]),
            asks: Mutex::new(vec![]),
        }
    }
}

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

struct CommandHandler;
impl CommandHandler {
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

    async fn handle_socket(tcp_stream: TcpStream, tx: UnboundedSender<Command>) {
        let mut buff_reader = BufReader::new(tcp_stream);
        loop {
            let mut msg = String::new();
            let read = buff_reader.read_line(&mut msg).await.unwrap(); // assume everything is OK

            // socket closed by client
            if read == 0 {
                break;
            }

            let command = CommandHandler::parse_command(msg);
            tx.send(command);
        }
    }
}

async fn run_auction(book: Arc<DoubleSidedBook>) {
    let mut bid_lock = book.bids.lock().unwrap();
    let mut ask_lock = book.asks.lock().unwrap();

    let report = calculate_batch(&mut bid_lock, &mut ask_lock);

    drop(bid_lock);
    drop(ask_lock);

    // run separate task, that will print to stdio
    tokio::spawn(async move {
        match report {
            BatchReport::NoTrade => println!("No Trade"),
            BatchReport::Trade {
                price,
                qty,
                cleared_bids,
                cleared_asks,
            } => {
                println!(
                    "Batch: cleared BID={}, cleared ASK={}, price={:?}, qty={}",
                    cleared_bids.len(),
                    cleared_asks.len(),
                    price,
                    qty
                );
            }
        }
    });
}

async fn update_order_book(mut rx: UnboundedReceiver<Command>, book: Arc<DoubleSidedBook>) {
    loop {
        let cmd = rx.recv().await.unwrap();

        let order = Order::new(cmd.price.clone(), cmd.qty);

        match cmd.order_type {
            OrderType::Buy => match cmd.command_type {
                CommandType::Add => {}
                CommandType::Cancel => {}
            },
            OrderType::Sell => match cmd.command_type {
                CommandType::Add => {}
                CommandType::Cancel => {}
            },
        }
    }
}

#[tokio::main]
async fn main() {
    // init order books
    let book = Arc::new(DoubleSidedBook::new());

    // init channel
    // pipeline: socket -> channel -> order book
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    //schedule periodic auction execution
    let _book = book.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(INTERVAL_MILLIS));
        loop {
            interval.tick().await;
            run_auction(_book.clone()).await;
        }
    });

    //init task for order book updates
    // channel -> order book
    tokio::spawn(update_order_book(rx, book.clone()));

    let mut tcp_listener = TcpListener::bind("0.0.0.0:7777").await.unwrap();

    loop {
        let (socket, _) = tcp_listener.accept().await.unwrap(); // assume everything is OK
        tokio::spawn(CommandHandler::handle_socket(socket, tx.clone()));
    }
}
