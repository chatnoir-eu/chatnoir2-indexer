#!/usr/bin/env bash

corpus="$1"
dataset="$2"
index_name="$3"
partitions="$4"
batches="$5"
keystore="$6"

set -e
trap "echo Exiting on request >&2; exit" SIGINT SIGTERM

if [[ "$corpus" != "cw" ]] && [[ "$corpus" != "cc" ]]; then
    echo "Invalid corpus '$corpus', valid choices are: cw, cc" >&2
    exit 1
fi

if [ -z "$dataset" ]; then
    echo "You need to specify a source dataset (09, 12, CC-MAIN-20...)." >&2
    exit 1
fi

if [ -z "$index_name" ]; then
    echo "You need to specify a target index name." >&2
    exit 1
fi

if [ -z "$partitions" ]; then
    echo "You need to specify the number of MapFile partitions." >&2
    exit 1
fi

if [ -z "$batches" ]; then
    echo "You need to specify the number of indexing batches." >&2
    exit 1
fi

if [ -z "$batches" ]; then
    echo "You need to specify the number of indexing batches." >&2
    exit 1
fi

shift 5


for batch in $(seq 1 ${batches}); do
    echo "Running batch ${batch} of ${batches}..."

    if [[ "$corpus" == "cw" ]] && [[ "$dataset" == "09" ]]; then
        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015.bw.webis.de,betaweb035.bw.webis.de,betaweb055.bw.webis.de,betaweb075.bw.webis.de,betaweb095.bw.webis.de,betaweb115.bw.webis.de \
            -Des.port=30920 \
            -Des.net.ssl=true \
            -Des.batch.size.entries=700 \
            -Des.batch.size.bytes=3mb \
            $@ \
            -uuid-prefix "clueweb${dataset}" \
            -sequence-files "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-mapfile/" \
            -partitions ${partitions} \
            -spamranks "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-spam-rankings/*" \
            -anchortexts "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-anchors/*" \
            -pageranks "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-page-ranks.txt" \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "${index_name}"
    elif [[ "$corpus" == "cw" ]] && [[ "$dataset" == "12" ]]; then
        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015.bw.webis.de,betaweb035.bw.webis.de,betaweb055.bw.webis.de,betaweb075.bw.webis.de,betaweb095.bw.webis.de,betaweb115.bw.webis.de \
            -Des.port=30920 \
            -Des.net.ssl=true \
            -Des.batch.size.entries=700 \
            -Des.batch.size.bytes=3mb \
            $@ \
            -uuid-prefix "clueweb${dataset}" \
            -sequence-files "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-mapfile/" \
            -partitions ${partitions} \
            -spamranks "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-spam-rankings/*" \
            -pageranks "/corpora/corpora-thirdparty/corpus-clueweb/${dataset}-page-ranks.txt.bz2" \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "${index_name}"
    elif [[ "$corpus" == "cc" ]]; then
        hadoop jar $(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar \
            "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015.bw.webis.de,betaweb035.bw.webis.de,betaweb055.bw.webis.de,betaweb075.bw.webis.de,betaweb095.bw.webis.de,betaweb115.bw.webis.de \
            -Des.port=30920 \
            -Des.net.ssl=true \
            -Des.batch.size.entries=700 \
            -Des.batch.size.bytes=3mb
            -Dmapreduce.job.split.metainfo.maxsize=-1 \
            $@ \
            -uuid-prefix "commoncrawl" \
            -sequence-files "/corpora/corpus-commoncrawl/${dataset}-mapfile/" \
            -partitions ${partitions} \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "${index_name}"
    fi
done
