#!/usr/bin/env bash

JAR_VERSION="1.0-SNAPSHOT"
corpus="$1"
index_revision="$2"

if [[ "$corpus" != "09" ]] && [[ "$corpus" != "12" ]]; then
    echo "Invalid corpus '$corpus', valid choices are: 09, 12, cc" >&2
    exit 1
fi

if [[ "$index_revision" == "" ]]; then
    echo "You need to specify an index revision." >&2
    exit 1
fi

if [[ "$corpus" == "09" ]]; then
    hadoop jar "$(dirname $0)/../../build/libs/es-indexer-${JAR_VERSION}.jar" "de.webis.chatnoir2.app.ESIndexer" \
        -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
        -sequence-files "/corpora/clueweb/${corpus}-mapfile/data-r-*/data" \
        -spamranks "/corpora/clueweb/${corpus}-spam-rankings/*" \
        -anchortexts "/corpora/clueweb/${corpus}-anchors/*" \
        -pageranks "/corpora/clueweb/${corpus}-page-ranks.txt" \
        -index "webis_warc_clueweb${corpus}_${index_revision}" $@
elif [[ "$corpus" == "12" ]]; then
    hadoop jar "$(dirname $0)/../../build/libs/es-indexer-${JAR_VERSION}.jar" "de.webis.chatnoir2.app.ESIndexer" \
        -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
        -sequence-files "/corpora/clueweb/${corpus}-mapfile/data-r-*/data" \
        -spamranks "/corpora/clueweb/${corpus}-spam-rankings/*" \
        -pageranks "/corpora/clueweb/${corpus}-page-ranks.txt.bz2" \
        -index "webis_warc_clueweb${corpus}_${index_revision}" $@
elif [[ "$corpus" == "cc" ]]; then
    echo "Not implemented yet." >&2
    exit 1
fi
