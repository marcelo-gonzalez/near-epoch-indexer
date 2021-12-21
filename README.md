# NEAR Epoch Indexer

This is an indexer meant to collect validator statistics from an RPC node and dump them into a postgres database. For now we just save the number of blocks produced and expected for each validator, but more stats could be added pretty easily.


## Build and run

We need the `diesel-cli` tool to create and manage the tables involved.

```bash
$ cargo install diesel_cli --no-default-features --features "postgres"
```

Assuming you have postgres installed, run:

```bash
$ createdb epochs-testnet
$ echo DATABASE_URL=postgresql://yourname:password@localhost/epochs-testnet >> .env
$ diesel migration run
```

Build the code...
```bash
$ sudo apt update && sudo apt-get install libpq-dev
$ cargo build --release
```

Run it with the `DATABASE_URL` environment variable set or listed in the file `./.env` as above.
```bash
$ ./target/release/near-epoch-indexer --rpc-url http://localhost:3030 --chain-id testnet
```
