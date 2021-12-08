CREATE TABLE public.epochs (
    epoch_id text PRIMARY KEY,
    height integer NOT NULL,
    start_timestamp bigint UNIQUE NOT NULL,
    end_timestamp bigint UNIQUE NOT NULL,
    start_height bigint UNIQUE NOT NULL,
    end_height bigint UNIQUE NOT NULL
    CHECK(start_timestamp < end_timestamp)
    CHECK(start_height < end_height)
);

CREATE TABLE public.validator_stats (
    account_id text NOT NULL,
    epoch_id text NOT NULL REFERENCES public.epochs (epoch_id) ON DELETE CASCADE,
    num_produced_blocks integer NOT NULL,
    num_expected_blocks integer NOT NULL,
    PRIMARY KEY (account_id, epoch_id)
);

CREATE UNIQUE INDEX start_timestamp_idx ON epochs (start_timestamp);
CREATE UNIQUE INDEX end_timestamp_idx ON epochs (end_timestamp);
