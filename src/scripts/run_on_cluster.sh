#!/usr/bin/env bash

corpus="$1"

if [[ "$corpus" != "09" ]] && [[ "$corpus" != "12" ]]; then
    echo "Invalid corpus '$corpus', valid choices are: 09, 12, cc" >&2
    exit 1
fi

if [[ "$corpus" == "09" ]] || [[ "$corpus" == "12" ]]; then
    hadoop jar "$(dirname $0)/../../build/classes/artifacts/es_indexer_jar/es-indexer.jar" "de.webis.chatnoir2.app.ESIndexer" \
        -Des.nodes==betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
        -sequence-files "/corpora/clueweb/${corpus}-mapfile/data-r-*/data" \
        -spamranks "/corpora/clueweb/${corpus}-spam-rankings/*" \
        -anchortexts "/corpora/clueweb/${corpus}-anchors/*" \
        -pageranks "/corpora/clueweb/${corpus}-page-ranks.txt" \
        -langdetect "betaweb020:9200" \
        -index "clueweb${corpus}-$(date -I)" $@
elif [[ "$corpus" == "cc" ]]; then
    echo "Not implemented yet." >&2
    exit 1
fi