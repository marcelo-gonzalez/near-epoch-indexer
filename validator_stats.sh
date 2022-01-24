#!/usr/bin/env bash

usage() {
    cat <<-EOF
	Usage:
	    validators.sh [options] <account ID>

	    Displays block/chunk production stats for the given validator ID. The script
	    expects the MAINNET_EPOCHS_DATABASE_URL and TESTNET_EPOCHS_DATABASE_URL environment
	    variables to be set.

	    Options:

	    -c <chain_id>: possible values are "testnet" or "mainnet". Default "mainnet"
	    -f <timestamp>: f as in "from". Results will only include epochs that ended
	       		    at or after this timestamp
	    -t <timestamp>: t as in "to". Results will only include epochs that began
	       		    at or before this timestamp

	    Example:

	    $ export TESTNET_EPOCHS_DATABASE_URL=postgresql://myuser:mypassword@localhost/testnet
	    $ sh validator_stats.sh -c testnet -f "2022-01-01 03:04:06" -t "2022-01-06 03:04:06" node0

	EOF
    exit $1
}

show_stats() {
    if [ "$chain_id" = "mainnet" ]; then
        if [ -z "$MAINNET_EPOCHS_DATABASE_URL" ]; then
            echo "please set the MAINNET_EPOCHS_DATABASE_URL environment variable"
            exit 1
        fi
        url=$MAINNET_EPOCHS_DATABASE_URL
    elif [ "$chain_id" = "testnet" ]; then
        if [ -z "$TESTNET_EPOCHS_DATABASE_URL" ]; then
            echo "please set the TESTNET_EPOCHS_DATABASE_URL environment variable"
            exit 1
        fi
        url=$TESTNET_EPOCHS_DATABASE_URL
    else
        usage 1
    fi

    query=$(
        cat <<EOF
SELECT to_timestamp(epochs.start_timestamp / 1000000000) AS start,
to_timestamp(epochs.end_timestamp / 1000000000) AS end, epochs.height,
validator_stats.num_produced_blocks, validator_stats.num_expected_blocks,
validator_stats.num_produced_chunks, validator_stats.num_expected_chunks
FROM validator_stats LEFT OUTER JOIN epochs ON (validator_stats.epoch_id = epochs.epoch_id)
WHERE validator_stats.account_id = '$account_id'
EOF
    )
    if [ "$from" != "none" ]; then
        query="${query} AND epochs.end_timestamp >=
DATE_PART('epoch', TIMESTAMP '${from}')::numeric::bigint * 1000000000"
    fi
    if [ "$to" != "none" ]; then
        query="${query} AND epochs.start_timestamp <=
DATE_PART('epoch', TIMESTAMP '${to}')::numeric::bigint * 1000000000"
    fi

    psql -c "$query ORDER BY height DESC;" "$url"
}

run() {
    if [ $# -lt 1 ]; then
        usage 1
        exit 1
    fi

    local chain_id="mainnet"
    local from="none"
    local to="none"

    while getopts "c:f:t:" o; do
        case "${o}" in
        c)
            chain_id=${OPTARG}
            ;;
        f)
            from=${OPTARG}
            ;;
        t)
            to=${OPTARG}
            ;;
        *)
            usage 1
            ;;
        esac
    done

    shift $((OPTIND - 1))
    if [ $# -ne 1 ]; then
        usage 1
    fi

    account_id=$1
    show_stats
}

case "$1" in
help | --help)
    usage 0
    ;;
*)
    run "$@"
    ;;
esac
