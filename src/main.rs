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
    epoch_id: CryptoHash,
    epoch_height: EpochHeight,
    // hash of the last block in the previous epoch
    prev_last_block: CryptoHash,
}

impl EpochRef {
    fn from_info(epoch: &EpochInfo) -> Self {
        Self {
            epoch_id: epoch.id,
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
        until: EpochHeight,
    ) -> anyhow::Result<Option<EpochInfo>> {
        if until >= epoch_ref.epoch_height - 1 {
            return Ok(None);
        }
        let epoch = match self.fetch_epoch(&epoch_ref.prev_last_block).await {
            Ok(Some(info)) => info,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };
        if until >= epoch.info.epoch_height {
            return Ok(None);
        }
        if epoch.info.epoch_height == epoch_ref.epoch_height {
            warn!(
                "Found an epoch height repeat! Epochs {} and {} both have height #{}. Skipping...",
                epoch.id, epoch_ref.epoch_id, epoch_ref.epoch_height
            );
            return Ok(Some(epoch));
        }
        if epoch.info.epoch_height > epoch_ref.epoch_height {
            warn!(
                "Epoch height sequence is descending from {} -> {}! (height #{} -> #{}). Skipping...",
                epoch.id, epoch_ref.epoch_id, epoch.info.epoch_height, epoch_ref.epoch_height);
            return Ok(Some(epoch));
        }
        if epoch.info.epoch_height != epoch_ref.epoch_height - 1 {
            warn!(
                "Found a gap in epoch heights! No epoch between #{} and #{} found.",
                epoch.info.epoch_height, epoch_ref.epoch_height
            );
        }
        self.save_validator_info(&epoch)?;
        Ok(Some(epoch))
    }

    fn read_indexed_epochs(
        &self,
        before: Option<EpochHeight>,
        limit: usize,
    ) -> anyhow::Result<Vec<(EpochHeight, BlockHeight)>> {
        let query = schema::epochs::table
            .select((schema::epochs::height, schema::epochs::start_height))
            .order_by(schema::epochs::height.desc());
        let rows = if let Some(before) = before {
            query
                .filter(schema::epochs::height.lt(i32::try_from(before).unwrap()))
                .limit(limit.try_into().unwrap())
                .load::<(i32, i64)>(&self.db)?
        } else {
            query
                .limit(limit.try_into().unwrap())
                .load::<(i32, i64)>(&self.db)?
        };

        if rows.len() > limit {
            return Err(anyhow!("SELECT height FROM epochs ORDER BY height DESC LIMIT {} unexpectedly returned {} rows...",
			       limit, rows.len()));
        }
        let mut ret = vec![];

        for row in rows.iter() {
            let height = match EpochHeight::try_from(row.0) {
                Ok(h) => h,
                Err(_) => {
                    return Err(anyhow!(
                        "epochs table seems to contain a negative height!: {}",
                        row.0
                    ));
                }
            };
            let start_height = match BlockHeight::try_from(row.1) {
                Ok(h) => h,
                Err(_) => {
                    return Err(anyhow!(
                        "epochs table seems to contain a negative block height!: {}",
                        row.1
                    ));
                }
            };
            ret.push((height, start_height));
        }
        Ok(ret)
    }

    fn read_num_epochs_indexed(&self) -> diesel::result::QueryResult<u64> {
        Ok(schema::epochs::table
            .count()
            .get_result::<i64>(&self.db)?
            .try_into()
            .unwrap())
    }

    async fn backfill(
        &mut self,
        start: EpochHeight,
        end: EpochHeight,
        end_first_block: BlockHeight,
    ) -> anyhow::Result<()> {
        let mut epoch_ref = match self.fetch_block_at_height(end_first_block).await {
            Ok(b) => EpochRef {
                epoch_id: b.header.epoch_id,
                epoch_height: end,
                prev_last_block: b.header.prev_hash,
            },
            Err(e) => return Err(e),
        };

        loop {
            let started_at = Instant::now();
            let start_req_count = self.req_count;

            let epoch = self
                .fetch_and_index_prev_epoch(&epoch_ref, start)
                .await
                .context(format!(
                    "Failed to index epoch #{}",
                    epoch_ref.epoch_height - 1
                ))?;

            match epoch {
                Some(epoch) => {
                    epoch_ref = EpochRef::from_info(&epoch);
                    std::thread::sleep(til_next_request(
                        self.options.backfill_qps,
                        started_at,
                        self.req_count - start_req_count,
                    ));
                }
                None => {
                    return Ok(());
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

    // returns true if there is more to go.
    async fn index_missing_rows(
        &mut self,
        rows: &[(EpochHeight, BlockHeight)],
        query_limit: usize,
        smallest_indexed: &mut (EpochHeight, BlockHeight),
        missing_rows: &mut u64,
        latest: EpochHeight,
        latest_start_height: BlockHeight,
    ) -> bool {
        for row in rows.iter() {
            let height = row.0;

            // the height == 1 check should not be needed since we check missing_rows,
            // but keep it as a paranoid check
            if height == 1 || *missing_rows == 0 {
                return false;
            }

            if height >= smallest_indexed.0 {
                error!(
                    "epochs query with \"ORDER BY height DESC\" not strictly descending: {} -> {}",
                    smallest_indexed.0, height
                );
                return false;
            }
            if height >= latest {
                *missing_rows -= smallest_indexed.0 - height - 1;
                *smallest_indexed = *row;
                continue;
            }
            if height < smallest_indexed.0 - 1 && height < latest - 1 {
                let result = if smallest_indexed.0 < latest {
                    self.backfill(height, smallest_indexed.0, smallest_indexed.1)
                        .await
                } else {
                    self.backfill(height, latest, latest_start_height).await
                };
                if let Err(e) = result {
                    error!("{:#}", e);
                    return false;
                }
                *missing_rows -= smallest_indexed.0 - height - 1;
            }
            *smallest_indexed = *row;
        }
        if rows.len() < query_limit {
            let result = if smallest_indexed.0 < latest {
                self.backfill(0, smallest_indexed.0, smallest_indexed.1)
                    .await
            } else {
                self.backfill(0, latest, latest_start_height).await
            };
            if let Err(e) = result {
                error!("{:#}", e);
            }
            false
        } else {
            true
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
        let num_rows = match self.read_num_epochs_indexed() {
            Ok(n) => n,
            Err(e) => {
                error!("Error reading num rows from the database: {}", e);
                return;
            }
        };
        let last_row = match self.read_indexed_epochs(None, 1) {
            Ok(rows) => rows.first().copied(),
            Err(e) => {
                error!("Error querying the database: {:#}", e);
                return;
            }
        };
        let last_indexed = match last_row {
            Some(row) => row.0,
            None => 0,
        };
        let mut missing_rows = 0;

        if latest_epoch.epoch_height > last_indexed + 1 {
            info!(
                "There are new epochs to index. Current epoch = #{}. Last indexed = #{}.",
                latest_epoch.epoch_height, last_indexed
            );
            missing_rows += latest_epoch.epoch_height - last_indexed - 1;
        }
        if last_indexed > num_rows {
            info!(
                "There seem to be some missing rows in the database. \
		 The last indexed epoch has height #{} but there are only \
		 {} rows. Attempting to backfill...",
                last_indexed, num_rows
            );
            missing_rows += last_indexed - num_rows;
        }
        if missing_rows == 0 {
            info!(
                "The latest completed epoch #{} has already been saved. Nothing to do.",
                last_indexed
            );
            return;
        }

        let mut smallest_indexed;

        if let Some(row) = last_row {
            let height = row.0;
            if height < latest_epoch.epoch_height - 1 {
                if let Err(e) = self
                    .backfill(
                        height,
                        latest_epoch.epoch_height,
                        latest_epoch.epoch_start_height,
                    )
                    .await
                {
                    error!("{:#}", e);
                    return;
                }

                missing_rows -= latest_epoch.epoch_height - height - 1;
                if missing_rows == 0 {
                    return;
                }
            } else if height >= latest_epoch.epoch_height {
                warn!(
                    "{} gave latest epoch height = #{}. \
		       Seems to be behind what has already been indexed (latest = #{})",
                    self.options.rpc_url, latest_epoch.epoch_height, height
                );
            }
            smallest_indexed = row;
        } else {
            if let Err(e) = self
                .backfill(
                    0,
                    latest_epoch.epoch_height,
                    latest_epoch.epoch_start_height,
                )
                .await
            {
                error!("{:#}", e);
            }
            return;
        }

        loop {
            let limit = 100;
            let heights = match self.read_indexed_epochs(Some(smallest_indexed.0), limit) {
                Ok(heights) => heights,
                Err(e) => {
                    error!(
                        "Error querying database for epochs earlier than #{}: {:#}",
                        smallest_indexed.0, e
                    );
                    return;
                }
            };
            if !self
                .index_missing_rows(
                    &heights,
                    limit,
                    &mut smallest_indexed,
                    &mut missing_rows,
                    latest_epoch.epoch_height,
                    latest_epoch.epoch_start_height,
                )
                .await
            {
                return;
            };
        }
    }
}

#[actix_web::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()
        .unwrap();

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
