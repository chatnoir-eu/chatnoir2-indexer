#!/usr/bin/env bash

corpus="$1"
index_revision="$2"

if [[ "$corpus" != "09" ]] && [[ "$corpus" != "12" ]] && [[ "$corpus" != "cc" ]]; then
    echo "Invalid corpus '$corpus', valid choices are: 09, 12, cc" >&2
    exit 1
fi

if [[ "$index_revision" == "" ]]; then
    echo "You need to specify an index revision." >&2
    exit 1
fi

if [[ "$corpus" == "09" ]]; then
    hadoop jar "$(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar" \
        "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
        -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
        -uuid-prefix "clueweb${corpus}" \
        -sequence-files "/corpora/clueweb/${corpus}-mapfile/" \
        -spamranks "/corpora/clueweb/${corpus}-spam-rankings/*" \
        -anchortexts "/corpora/clueweb/${corpus}-anchors/*" \
        -pageranks "/corpora/clueweb/${corpus}-page-ranks.txt" \
        -index "webis_warc_clueweb${corpus}_${index_revision}" $@
elif [[ "$corpus" == "12" ]]; then
    hadoop jar "$(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar" \
        "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
        -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
        -uuid-prefix "clueweb${corpus}" \
        -sequence-files "/corpora/clueweb/${corpus}-mapfile/" \
        -spamranks "/corpora/clueweb/${corpus}-spam-rankings/*" \
        -pageranks "/corpora/clueweb/${corpus}-page-ranks.txt.bz2" \
        -index "webis_warc_clueweb${corpus}_${index_revision}" $@
elif [[ "$corpus" == "cc" ]]; then
    partitions=100
    batches=4

    for batch in $(seq 1 4); do
        echo "Running batch ${batch} of 4..."
        hadoop jar "$(dirname $0)/../../build/libs/chatnoir2-indexer-*-all.jar" \
        "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
            -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
            -Des.batch.size.entries=7000 \
            -Des.batch.size.bytes=10mb \
            -Dmapreduce.job.split.metainfo.maxsize=-1 \
            -uuid-prefix "commoncrawl" \
            -sequence-files "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/" \
            -partitions ${partitions} \
            -batches ${batches} \
            -batch-num ${batch} \
            -index "webis_warc_commoncrawl15_${index_revision}" $@

        #echo "Merging down segments..."
        #curl -XPOST "http://betaweb015:9200/webis_warc_commoncrawl15_${index_revision}/_forcemerge?max_num_segments=1"
    done
fi
