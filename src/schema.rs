table! {
    epochs (epoch_id) {
        epoch_id -> Text,
        height -> Int4,
        start_timestamp -> Int8,
        end_timestamp -> Int8,
    }
}

table! {
    validator_stats (account_id, epoch_id) {
        account_id -> Text,
        epoch_id -> Text,
        num_produced_blocks -> Int4,
        num_expected_blocks -> Int4,
    }
}

joinable!(validator_stats -> epochs (epoch_id));

allow_tables_to_appear_in_same_query!(epochs, validator_stats,);
