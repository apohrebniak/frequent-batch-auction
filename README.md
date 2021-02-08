## Basic Frequent Batch Auction implementation
* works only for one ticker
* there is no "order identifier". CANCELs works by direct match with order in order book
* order may be cleared partially

### Message Format
```
<ADD|CANCEL>,<BUY|SELL>,<price>,<qty>
```

### Example
```
$ telnet localhost 7777
ADD,SELL,43.52.10
ADD,BUY,43.55,5
```

### bench auction function (requires nightly)
 - spread 140-150
 - qty per order [1, 200)
```
cargo bench
```