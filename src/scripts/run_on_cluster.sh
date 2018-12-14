#!/usr/bin/env bash

corpus="$1"
dataset="$2"
index_revision="$3"
batches="$4"
partitions="$5"

trap "echo Exiting on request >&2; exit" SIGINT SIGTERM

if [[ "$corpus" != "cw" ]] && [[ "$corpus" != "cc" ]]; then
    echo "Invalid corpus '$corpus', valid choices are: cw, cc" >&2
    exit 1
fi

if [[ "$dataset" == "" ]]; then
    echo "You need to specify a source dataset (09, 12, CC-MAIN-20...)." >&2
    exit 1
fi

if [[ "$index_revision" == "" ]]; then
    echo "You need to specify an index revision." >&2
    exit 1
fi

if [[ "$partitions" == "" ]]; then
    echo "You need to specify the number of partitions." >&2
    exit 1
fi

if [[ "$batches" == "" ]]; then
    echo "You need to specify the number of indexing batches." >&2
    exit 1
fi

shift 5


for batch in $(seq 1 ${batches}); do
    echo "Running batch ${batch} of ${batches}..."

    if [[ "$corpus" == "cw" ]] && [[ "$dataset" == "09" ]]; then
        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
            -uuid-prefix "clueweb${dataset}" \
            -sequence-files "/corpora/clueweb/${dataset}-mapfile/" \
            -spamranks "/corpora/clueweb/${dataset}-spam-rankings/*" \
            -anchortexts "/corpora/clueweb/${dataset}-anchors/*" \
            -pageranks "/corpora/clueweb/${dataset}-page-ranks.txt" \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "webis_warc_clueweb${dataset}_${index_revision}" $@
    elif [[ "$corpus" == "cw" ]] && [[ "$dataset" == "12" ]]; then
        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
            -uuid-prefix "clueweb${dataset}" \
            -sequence-files "/corpora/clueweb/${dataset}-mapfile/" \
            -spamranks "/corpora/clueweb/${dataset}-spam-rankings/*" \
            -pageranks "/corpora/clueweb/${dataset}-page-ranks.txt.bz2" \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "webis_warc_clueweb${dataset}_${index_revision}" $@
    elif [[ "$corpus" == "cc" ]]; then
        index_name="$(echo ${dataset} | awk '{print tolower($0)}' | sed s/cc-main-20// | sed s/-//g)"

        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
            -Des.batch.size.entries=7000 \
            -Des.batch.size.bytes=10mb \
            -Dmapreduce.job.split.metainfo.maxsize=-1 \
            -uuid-prefix "commoncrawl" \
            -sequence-files "/corpora/corpus-commoncrawl/${dataset}-mapfile/" \
            -partitions ${partitions} \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "webis_warc_commoncrawl${index_name}_${index_revision}" $@

        #echo "Merging down segments..."
        #curl -XPOST "http://betaweb015:9200/webis_warc_commoncrawl15_${index_revision}/_forcemerge?max_num_segments=1"
    fi
done
