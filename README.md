# Chatnoir2 Indexer

Hadoop MapReduce tool for indexing Webis WARC MapFiles into an Elasticsearch/Chatnoir2 index.

## Indexing Process
The indexer will create an index automatically if it doesn't exist, but in order for the index to work properly,
we want to adjust some settings first as described below.

**NOTE:** in the following text the placeholder `{{eshost}}` stands for the ElasticSearch hostname and port
 that you want to index to (e.g. `localhost:9200` or `betaweb020:9200`.
 Similarly, `{{index}}` stands for the name of your index (e.g. `webis_warc_clueweb12_001`).

### 1. Set Indexing Template
For an appropriate field mapping and proper analyzer choice we use the indexing template located in
`src/main/resources/templates/webis_warc_template.json`. The template makes sure that Elasticsearch
uses correct data types for our fields and also specifies ICU tokenizers, stop words and suitable analyzers
for different content languages. The template applies to any index whose name starts with
`webis_warc_*`. If you want to name your index differently, change the first property in the JSON file
accordingly.

In order to make our template known to ElasticSearch, we have to PUT it to the `_template` endpoint:

    curl -XPUT 'http://{{eshost}}/_template/webis_warc_template' \
        -d '@src/main/resources/templates/webis_warc_template.json'
        

To verify that the template has been saved to the Elasticsearch cluster, open
`http://{{eshost}}/_template/webis_warc_template?pretty` in your browser. You should see a JSON dump
of the template we just sent.

### 2. Create Index
For creating our index, we use the following cURL snippet:

    curl -XPUT 'http://{{eshost}}/{{index}}/' -d '
    {
         "settings" : {
             "index" : {
                 "number_of_shards" : 30,
                 "number_of_replicas" : 0
             }
         }
     }'

This will create an index with 30 shards and 0 replica named `{{index}}`. The actual number of shards depends on the
size of your cluster and also on the amount of data you want to index. For ClueWeb09/12 I found 30 to be a working
number, but there is no fixed rationale behind it. There are certain rules of thumb about how to chose the correct
number of shards, but in general it's just experience. Bear in mind, though, that too many shards can cause considerable
overhead while too few don't exploit the capabilities of a cluster with many nodes. You should take some time to think
about the number of shards before your start indexing because once your index is created, you can't change it anymore
without re-indexing all your data.

One thing we can and will change later on, though, is the number of replica. It is advisable to set this to a moderate
number like 2 or 3 (2 means 1 primary shard and 2 replica, i.e. 3 copies in total). The number of replica is a tradeoff
between fail-over/data loss safety and query performance on the one hand and disk usage on the other hand.

For indexing purposes, though, we want to set it to 0 because we want to use our cluster resources (both CPU time
and I/O bandwidth) to index our data and not to constantly replicate and re-balance what we just indexed throughout
the cluster. Once your data has been indexed, activate the replica (in this case: 2) using

    curl -XPUT 'http://{{eshost}}/{{index}}/_settings' -d '
    {
        "index" : {
            "number_of_replicas" : 2
        }
    }'

### 3. Start Indexing Process
To start the indexer, we use the `hadoop` command to run this Java tool. Make sure, you set the number of reduces to
something sensible before starting it using your local `mapred.xml` config file. The default of 1 is definitely too
little. A number between 40 and 100 (depending on the cluster size) seems to be sensible. As long as the Elasticsearch
indexing host(s) can handle that many parallel indexing requests, you can increase the number as you like.

We start the indexing with

    hadoop jar es-indexer.jar de.webis.chatnoir2.app.ESIndexer \
        -Des.nodes="{{eshost}}" \
        -sequence-files "/corpus-path/mapfile/data-r-*/data" \
        -spamranks "/corpus-path/spam-rankings/*" \
        -pageranks "/corpus-path/page-ranks.txt" \
        -anchortexts "/corpus-path/anchors/*" \
        -index "{{index}}"

`-Des.nodes` is a comma separated list of indexing endpoints (with optional port number, default is 9200).
In this example, it is just `{{eshost}}`, but it is strongly advised to use more than one. Usually you want to have
a certain class of hosts that don't store any data themselves but only answer search requests and accept data
to index. It is perfectly fine, the specify data nodes here as well, but for performance reasons your may want to
have separate *coordinator* nodes which you should specify here.

`-sequence-files` is the HDFS path to your mapfile splits. `-spamranks` specifies the path to your spam ranks
(a file with the format `<ID> <NUMBER>`). `-pageranks` is similar, but for page ranks, of course.
`-anchortexts` are your anchor texts for certain documents (format
`<ID> <TEXT>`, where `<TEXT>` will be cut off after a certain amount of characters during indexing).
Last, but not least, `-index` names your actual index (the one we created before).

Depending on the amount of data and the performance of your cluster, the MapReduce job may run for several hours or
even days while your data is continually fed into the index.
You can follow the process using the Hadoop Application web interface as well as the Elasticsearch JSON search API.
You can also install a tool such as [Elastic HQ](http://www.elastichq.org/) on your cluster to get more information
about your indexes, the number of already indexed documents as well the performance measures of the nodes.