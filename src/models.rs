use crate::schema::{epochs, validator_stats};

use near_primitives::hash::CryptoHash;
use near_primitives::views::CurrentEpochValidatorInfo;

#[derive(Insertable, Queryable)]
pub struct Epoch {
    pub epoch_id: String,
    pub height: i32,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub start_height: i64,
    pub end_height: i64,
}

impl Epoch {
    pub(crate) fn new(epoch: &crate::EpochInfo) -> Self {
        Self {
            epoch_id: epoch.id.to_string(),
            height: i32::try_from(epoch.info.epoch_height).unwrap_or(i32::MAX),
            start_timestamp: i64::try_from(epoch.first_block.header.timestamp).unwrap_or(i64::MAX),
            end_timestamp: i64::try_from(epoch.last_block.header.timestamp).unwrap_or(i64::MAX),
            start_height: i64::try_from(epoch.first_block.header.height).unwrap_or(i64::MAX),
            end_height: i64::try_from(epoch.last_block.header.height).unwrap_or(i64::MAX),
        }
    }
}

#[derive(Insertable)]
pub struct ValidatorStat {
    pub account_id: String,
    pub epoch_id: String,
    pub num_produced_blocks: i32,
    pub num_expected_blocks: i32,
}

impl ValidatorStat {
    pub(crate) fn new(epoch_id: &CryptoHash, info: &CurrentEpochValidatorInfo) -> Self {
        Self {
            account_id: info.account_id.to_string(),
            epoch_id: epoch_id.to_string(),
            num_produced_blocks: i32::try_from(info.num_produced_blocks).unwrap_or(i32::MAX),
            num_expected_blocks: i32::try_from(info.num_expected_blocks).unwrap_or(i32::MAX),
        }
    }
}
