# ChatNoir2 Indexer

Hadoop MapReduce tool for indexing Webis WARC MapFiles into a ChatNoir2 index.
If you haven't parsed your raw WARC files into WARC MapFiles yet, you need to do that first using the
mapfile-generator tool.

## Compiling the Source Code
Create a folder `aitools` next to this source directory and clone the following dependencies to it:

* [`aitools4-aq-web-page-content-extraction`](https://github.com/chatnoir-eu/aitools4-aq-web-page-content-extraction)
* [`aitools3-ie-languagedetection`](https://github.com/chatnoir-eu/aitools3-ie-languagedetection)
* [`aitools3-ie-stopwords`](https://github.com/chatnoir-eu/aitools3-ie-stopwords)

Then run
```
gradle shadowJar
```
from this source directory to download other third-party dependencies and compile the sources.

The generated shadow (fat) JAR will be in `build/libs`. The JAR can be submitted to run on a Hadoop cluster.
For ease of use, there is a helper script `src/scripts/run_on_cluster.sh` for starting the indexing process.

## Indexing Process
The indexer will create an index automatically if it doesn't exist, but in order for the index to work properly,
we want to adjust some settings first as described below.

**NOTE:** in the following text the placeholder `{{eshost}}` stands for the Elasticsearch hostname and port
that you want to index to (e.g. `localhost:9200` or `betaweb020:9200`.
Similarly, `{{index}}` stands for the name of your index (e.g. `webis_warc_clueweb12_001`).

### 1. Installing Needed Elasticsearch Plug-ins
To support as many languages as possible as well as for automatic language detection, some non-standard
Elasticsearch plug-ins need to be installed on all (!) cluster nodes. These are:

* `analysis-icu`
* `analysis-kuromoji`
* `analysis-smartcn`
* `analysis-stempel`

Assuming an Elasticsearch 5.x cluster, install them on each node using:

    bin/plugin install analysis-icu
    bin/plugin install analysis-kuromoji
    bin/plugin install analysis-smartcn
    bin/plugin install analysis-stempel

Restart every node after the installation.

### 2. Setting the Indexing Template
For an appropriate field mapping and proper analyzer choice we use the indexing template located in
`src/main/resources/templates/webis_warc_template.json`. The template makes sure that Elasticsearch
uses correct data types for our fields and specifies ICU tokenizers, stop words and suitable analyzers
for different content languages. It also creates dynamic mappings for unknown fields that are named
`*_lang.xy` to use the correct language by default. So, e.g., a new field `body_lang.en` would be indexed as
English while a field `body_lang.es` would be indexed as Spanish. This also is the reason why the template
does not contain any explicit field mappings for content, title, anchor texts, meta description etc.

The template applies to any index whose name starts with `webis_warc_*`. If you want to name your
index differently, change the first property in the JSON file accordingly.

In order to make our template known to Elasticsearch, we have to PUT it to the `_template` endpoint:

    curl -XPUT 'http://{{eshost}}/_template/webis_warc_template' \
        -d '@src/main/resources/templates/webis_warc_template.json'
        

To verify that the template has been saved to the Elasticsearch cluster, open
`http://{{eshost}}/_template/webis_warc_template?pretty` in your browser. You should see a JSON dump
of the template we just sent.

### 3. Creating the Index
For creating the index, use the following cURL snippet:

    curl -XPUT 'http://{{eshost}}/{{index}}/' -d '
    {
         "settings" : {
             "index" : {
                 "number_of_shards" : 40,
                 "number_of_replicas" : 0
             }
         }
     }'

This will create an index with 40 shards and 0 replica named `{{index}}`. The actual number of shards depends on the
size of your cluster and also on the amount of data you want to index. A good number is the number of data nodes in your
cluster divided by three (to achieve full allocation once we enable replicas).

It is also a good idea to set the refresh time to a high value (or disable it completely with -1):

    curl -X PUT  'http://{{eshost}}/{{index}}/_settings ' -d '
    {
        "index" : {
            "refresh_interval" : "3600s"
        }
    }'

For indexing purposes, setting the number of replicas to 0 is recommended, because we want to use our cluster
resources (both CPU time and I/O bandwidth) to index. Once your data has been indexed, activate the replica with

    curl -XPUT 'http://{{eshost}}/{{index}}/_settings' -d '
    {
        "index" : {
            "number_of_replicas" : 2
        }
    }'

### 4. Compiling the Indexer JAR
The indexer can be compiled with `gradle shadowJar`, which will compile a fat JAR suitable for submission to Hadoop.

### 5. Starting the Indexing Process
To start the indexer, use the `hadoop` command to run this Java tool. Make sure you set the number of reduces to
something sensible before starting it using your local `mapred-site.xml` config file. The default number of 1 is
definitely too small. A number between 40 and 100 (depending on the cluster size) seems to be sensible. As long as the
Elasticsearch indexing host(s) can handle that many parallel indexing requests, you can increase the number as you like.

For indexing the ClueWeb corpora, most default Hadoop settings should be fine. However, for indexing the larger
CommonCrawl, special tweaks may be needed. The following mapred-site.xml override config turned out to be working
quite well:

    <?xml version="1.0" encoding="UTF-8"?>    
    <configuration>
        <property>
            <name>mapreduce.framework.name</name>
            <value>yarn</value>
        </property>
        <property>
            <!-- We don't want our whole job to fail only because a few mappers died. -->
            <name>mapreduce.map.failures.maxpercent</name>
            <value>2</value>
        </property>
        <property>
            <!-- Keep this as low as possible, but don't turn it much lower than this. -->
            <name>mapreduce.map.memory.mb</name>
            <value>3072</value>
        </property>
        <property>
            <!-- Since we only have a fixed number of reduces, give them enough
                 memory to process data fast and without failures. -->
            <name>mapreduce.reduce.memory.mb</name>
            <value>4096</value>
        </property>
        <property>
            <!-- Adjust this to the number of nodes in your Hadoop cluster and the number
                 of indexing nodes in your ES cluster. Remember: the number of parallel
                 indexing requests is this number times the indexing batch size. The batch
                 size is defined by es.batch.size.entries and es.batch.size.bytes (whichever
                 is smaller) and defaults to 5000 and 2mb. This number should never be higher
                 than the reduce slots (or nodes) in your cluster. If your ES cluster is
                 overloaded, first reduce the batch size, then the number of reduces. -->
            <name>mapreduce.job.reduces</name>
            <value>100</value>
        </property>
        <property>
            <!-- Only run one reduce task per task tracker. -->
            <name>mapreduce.tasktracker.reduce.tasks.maximum</name>
            <value>1</value>
        </property>
        <property>
            <!-- Reuse JVMs. You may want to turn this off for very large jobs. -->
            <name>mapreduce.job.reuse.jvm.num.tasks</name>
            <value>-1</value>
        </property>
        <property>
            <!-- Give application manager enough memory. -->
            <name>yarn.app.mapreduce.am.command-opts</name>
            <value>-Xmx4096m</value>
        </property>
        <property>
            <!-- Give resource manager enough memory. This is especially important
                 for indexing the CommonCrawl. You may even want to up this even
                 further to 8192. -->
            <name>yarn.app.mapreduce.am.resource.mb</name>
            <value>5120</value>
        </property>
        <property>
            <!-- Start shuffling map outputs early. -->
            <name>mapreduce.job.reduce.slowstart.completedmaps</name>
            <value>0.05</value>
        </property>
    </configuration>

Once everything is configured, start the indexing process with

    hadoop jar chatnoir2-indexer.jar de.webis.chatnoir2.app.ChatNoirIndexer \
        -Des.nodes="{{eshost}}" \
        -sequence-files "/corpus-path/mapfile/data-r-*/data" \
        -spamranks "/corpus-path/spam-rankings/*" \
        -pageranks "/corpus-path/page-ranks.txt" \
        -anchortexts "/corpus-path/anchors/*" \
        -index "{{index}}"

`-Des.nodes` is a comma separated list of indexing endpoints (with optional port number, default is 9200).
In this example, it is just `{{eshost}}`, but it is strongly advised to use more than one. Usually you want to have
a certain number of hosts that don't store any data but only answer search requests and accept data to index.

`-sequence-files` is the HDFS glob pattern to your MapFile splits. `-spamranks` specifies the path to your spam ranks
(a file with the format `<ID> <NUMBER>`). `-pageranks` is similar, but for page ranks, of course.
`-anchortexts` are your anchor texts for certain documents (format
`<ID> <TEXT>`, where `<TEXT>` will be cut off after a certain amount of characters during indexing).
Last but not least, `-index` names your actual index (the one we created before).

You can also index a corpus in multiple batches by specifying the number of partitions of the input MapFile with
`-partitions`, the number of total batches with `-batches` and the current batch number with `-batch_num`.

Depending on the amount of data and the performance of your cluster, the MapReduce job may run for several hours or
even days while your data is continually fed into the index.
You can follow the process using the Hadoop Application web interface as well as the Elasticsearch X-Pack monitoring
tool in Kibana (if set up)).

**NOTE:** For starting the indexing process, there is a convenience script at `src/scripts/run_on_cluster.sh`
which makes it easier to index the CommonCrawl and ClueWeb corpora from HDFS (you may need to adjust the paths, though).
