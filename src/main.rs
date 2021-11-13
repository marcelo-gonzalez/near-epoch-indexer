use actix_web::client::Client;
use anyhow::{anyhow, Context};
use clap::{value_t, App, Arg};
use diesel::pg::PgConnection;
use http::StatusCode;
use log::{error, info, warn};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochHeight};
use near_primitives::views::{BlockView, EpochValidatorInfo};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use std::time::{Duration, Instant};

#[macro_use]
extern crate diesel;

use diesel::prelude::*;

pub mod models;
pub mod schema;

#[derive(Debug, Deserialize, Serialize)]
struct JSONError {
    code: i32,
    message: String,
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct JSONRpcResponse<T> {
    jsonrpc: String,
    result: Option<T>,
    error: Option<JSONError>,
    id: Value,
}

#[derive(Debug)]
enum JSONRpcError {
    RPC(JSONError),
    Parse,
}

impl std::fmt::Display for JSONRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JSONRpcError::RPC(e) => match serde_json::ser::to_string(e) {
                Ok(json) => f.write_str(&json),
                Err(_) => write!(f, "{:?}", &e),
            },
            JSONRpcError::Parse => {
                write!(
                    f,
                    "Received bad JSONRPC response: no \"result\" or \"error\" given"
                )
            }
        }
    }
}

impl std::error::Error for JSONRpcError {}

#[derive(Clone, Debug)]
struct Options {
    chain_id: String,
    rpc_url: String,
    backfill_qps: u32,
}

#[derive(Debug)]
struct EpochInfo {
    id: CryptoHash,
    info: EpochValidatorInfo,
    first_block: BlockView,
    last_block: BlockView,
}

#[derive(Debug)]
struct EpochRef {
    epoch_height: EpochHeight,
    // hash of the last block in the previous epoch
    prev_last_block: CryptoHash,
}

impl EpochRef {
    fn from_info(epoch: &EpochInfo) -> Self {
        Self {
            epoch_height: epoch.info.epoch_height,
            prev_last_block: epoch.first_block.header.prev_hash,
        }
    }
}

struct EpochIndexer {
    options: Options,
    db: PgConnection,
    client: Client,
    req_count: u32,
}

fn til_next_request(max_qps: u32, started_at: Instant, reqs_sent: u32) -> Duration {
    let dur = Instant::now() - started_at;
    if dur >= Duration::from_secs(1) {
        return Duration::from_secs(0);
    }
    let mut min_dur = Duration::from_secs(1) * reqs_sent;
    min_dur /= max_qps;
    if min_dur < dur {
        Duration::from_secs(0)
    } else {
        min_dur - dur
    }
}

// SendRequestError is not Send or Sync, so can't be made into an anyhow::Error :(
fn anyhow_from_actix(e: actix_http::client::SendRequestError) -> anyhow::Error {
    anyhow!("{}", e)
}

impl EpochIndexer {
    fn new(options: &Options, db: PgConnection) -> Self {
        Self {
            options: options.clone(),
            db,
            client: Default::default(),
            req_count: 0,
        }
    }

    async fn call_jsonrpc<T: serde::de::DeserializeOwned>(
        &mut self,
        method: &str,
        params: &Value,
    ) -> anyhow::Result<T> {
        self.req_count += 1;

        let mut response = match self
            .client
            .post(&self.options.rpc_url)
            .header("User-Agent", "near-validator-indexer")
            .send_json(&json!({
                "jsonrpc": "2.0",
                "method": method,
                "id": "dontcare",
                "params": params,
            }))
            .await
        {
            Ok(r) => match r.status() {
                StatusCode::OK => r,
                code => {
                    return Err(anyhow!(
                        "Call to JSONRPC method \"{}\" with params {} at {} returned HTTP {}",
                        method,
                        params,
                        &self.options.rpc_url,
                        code
                    ))
                }
            },
            Err(e) => {
                return Err(anyhow_from_actix(e));
            }
        };
        let json = response.json::<JSONRpcResponse<T>>().await?;
        match json.result {
            Some(result) => {
                return Ok(result);
            }
            None => match json.error {
                Some(error) => return Err(JSONRpcError::RPC(error).into()),
                None => return Err(JSONRpcError::Parse.into()),
            },
        }
    }

    async fn get_latest_epoch_info(&mut self) -> anyhow::Result<EpochValidatorInfo> {
        self.call_jsonrpc("validators", &json!({ "latest": Value::Null }))
            .await
    }

    async fn get_epoch_info(&mut self, id: &CryptoHash) -> anyhow::Result<EpochValidatorInfo> {
        self.call_jsonrpc("validators", &json!({ "epoch_id": id }))
            .await
    }

    async fn fetch_block_at_height(&mut self, height: BlockHeight) -> anyhow::Result<BlockView> {
        self.call_jsonrpc("block", &json!({ "block_id": height }))
            .await
    }

    async fn fetch_block(&mut self, hash: &CryptoHash) -> anyhow::Result<BlockView> {
        self.call_jsonrpc("block", &json!({ "block_id": hash }))
            .await
    }

    fn save_validator_info(&mut self, epoch_info: &EpochInfo) -> Result<(), diesel::result::Error> {
        let epoch = models::Epoch::new(epoch_info);
        diesel::insert_into(schema::epochs::table)
            .values(epoch)
            .on_conflict_do_nothing()
            .execute(&self.db)?;
        let mut validators = Vec::new();
        for v in epoch_info.info.current_validators.iter() {
            validators.push(models::ValidatorStat::new(&epoch_info.id, &v));
        }
        diesel::insert_into(schema::validator_stats::table)
            .values(validators)
            .on_conflict_do_nothing()
            .execute(&self.db)?;
        info!(
            "Successfully indexed epoch #{}, starting at #{} ending at #{}",
            epoch_info.info.epoch_height,
            epoch_info.first_block.header.height,
            epoch_info.last_block.header.height,
        );
        Ok(())
    }

    async fn fetch_epoch(
        &mut self,
        last_block_hash: &CryptoHash,
    ) -> anyhow::Result<Option<EpochInfo>> {
        if *last_block_hash == Default::default() {
            return Ok(None);
        }

        let last_block = self.fetch_block(last_block_hash).await?;
        if last_block.header.prev_hash == Default::default() {
            return Ok(None);
        }
        let info = match self.get_epoch_info(&last_block.header.epoch_id).await {
            Ok(info) => info,
            Err(e) => return Err(e),
        };
        let first_block = self.fetch_block_at_height(info.epoch_start_height).await?;
        Ok(Some(EpochInfo {
            id: last_block.header.epoch_id,
            info,
            first_block,
            last_block,
        }))
    }

    async fn fetch_and_index_prev_epoch(
        &mut self,
        epoch_ref: &EpochRef,
        last_indexed: EpochHeight,
    ) -> anyhow::Result<Option<EpochInfo>> {
        if last_indexed >= epoch_ref.epoch_height - 1 {
            return Ok(None);
        }
        let epoch = match self.fetch_epoch(&epoch_ref.prev_last_block).await {
            Ok(Some(info)) => info,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };
        if last_indexed >= epoch.info.epoch_height {
            return Ok(None);
        }
        self.save_validator_info(&epoch)?;
        Ok(Some(epoch))
    }

    fn read_last_indexed_epoch(&self) -> anyhow::Result<EpochHeight> {
        let last_indexed = schema::epochs::table
            .select(schema::epochs::height)
            .order_by(schema::epochs::height.desc())
            .limit(1)
            .load::<i32>(&self.db)?;
        if last_indexed.len() == 0 {
            return Ok(0);
        }
        if last_indexed.len() > 1 {
            warn!("SELECT height FROM epochs ORDER BY height DESC LIMIT 1 unexpectedly returned more than 1 row...: {:?}", last_indexed);
        }
        Ok(EpochHeight::try_from(last_indexed[0]).unwrap_or_else(|_| {
            error!(
                "epochs table seems to contain a negative height!: {}",
                last_indexed[0]
            );
            0
        }))
    }

    async fn backfill(&mut self, mut epoch_ref: EpochRef) {
        let last_indexed = match self.read_last_indexed_epoch() {
            Ok(last) => last,
            Err(e) => {
                error!("Error reading last indexed epoch from the database: {}", e);
                return;
            }
        };

        if last_indexed >= epoch_ref.epoch_height - 1 {
            info!(
                "The latest completed epoch #{} has already been saved. Nothing to do.",
                last_indexed
            );
            return;
        } else {
            info!(
                "The latest epoch is #{}, the last indexed was #{}",
                epoch_ref.epoch_height, last_indexed
            );
        }

        let mut did_something = false;

        loop {
            let started_at = Instant::now();
            let start_req_count = self.req_count;

            let epoch = match self
                .fetch_and_index_prev_epoch(&epoch_ref, last_indexed)
                .await
            {
                Ok(epoch) => epoch,
                Err(e) => {
                    // TODO: maybe try to keep going and save earlier ones
                    error!("Error indexing validator info: {}", e);
                    return;
                }
            };

            match epoch {
                Some(epoch) => {
                    did_something = true;
                    epoch_ref = EpochRef::from_info(&epoch);
                    std::thread::sleep(til_next_request(
                        self.options.backfill_qps,
                        started_at,
                        self.req_count - start_req_count,
                    ));
                }
                None => {
                    if !did_something {
                        info!("The latest completed epoch has already been saved. Nothing to do.");
                    }
                    return;
                }
            };
        }
    }

    async fn check_chain_id(&mut self) -> anyhow::Result<()> {
        let mut res = match self
            .client
            .get(format!("{}/status", &self.options.rpc_url))
            .header("User-Agent", "near-validator-indexer")
            .send()
            .await
        {
            Ok(req) => match req.status() {
                StatusCode::OK => req,
                code => {
                    return Err(anyhow!(
                        "{}/status returned {}",
                        &self.options.rpc_url,
                        code
                    ))
                }
            },
            Err(e) => return Err(anyhow_from_actix(e)),
        };
        let response = res.body().await?;
        let json = serde_json::from_slice::<Value>(&response).context(format!(
            "Parsing {}/status body:\n{}",
            &self.options.rpc_url,
            String::from_utf8_lossy(response.as_ref())
        ))?;
        match json.get("chain_id") {
            Some(id) => match id.as_str() {
                Some(id) => {
                    if id == self.options.chain_id {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "mismatch: {}/status: {}, ours: {}, ",
                            &self.options.rpc_url,
                            id.to_string(),
                            &self.options.chain_id
                        ))
                    }
                }
                None => Err(anyhow!(
                    "bad \"chain_id\" at {}/status: {}",
                    self.options.rpc_url,
                    id.to_string()
                )),
            },
            None => Err(anyhow!(
                "no \"chain_id\" at {}/status",
                self.options.rpc_url
            )),
        }
    }

    async fn run(&mut self) {
        if let Err(e) = self.check_chain_id().await {
            error!("Error checking chain ID: {}", e);
            return;
        }

        let latest_epoch = match self.get_latest_epoch_info().await {
            Ok(info) => info,
            Err(e) => {
                error!("Error getting validator info: {}", e);
                return;
            }
        };

        let first_block = match self
            .fetch_block_at_height(latest_epoch.epoch_start_height)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                error!(
                    "Error fetching block #{}: {}",
                    latest_epoch.epoch_start_height, e
                );
                return;
            }
        };

        self.backfill(EpochRef {
            epoch_height: latest_epoch.epoch_height,
            prev_last_block: first_block.header.prev_hash,
        })
        .await;
    }
}

#[actix_web::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    // TODO: add option to scan entire table and index any epochs not already in there.
    let matches = App::new("epoch-indexer")
        .arg(
            Arg::with_name("rpc-url")
                .long("rpc-url")
                .takes_value(true)
                .default_value("http://localhost:3030")
                .value_name("url"),
        )
        .arg(
            Arg::with_name("backfill-max-qps")
                .long("backfill-max-qps")
                .takes_value(true)
                .value_name("qps")
                .default_value("10")
                .help("maximum number of queries per second to make to the RPC node"),
        )
        .arg(
            Arg::with_name("chain-id")
                .long("chain-id")
                .takes_value(true)
                .value_name("chain")
                .required(true),
        )
        .get_matches();
    let options = Options {
        chain_id: matches.value_of("chain-id").unwrap().to_string(),
        rpc_url: matches.value_of("rpc-url").unwrap().to_string(),
        backfill_qps: clap::value_t!(matches, "backfill-max-qps", u32).unwrap(),
    };
    dotenv::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut indexer = EpochIndexer::new(
        &options,
        PgConnection::establish(&database_url)
            .expect(&format!("Error connecting to {}", database_url)),
    );
    indexer.run().await;
}
