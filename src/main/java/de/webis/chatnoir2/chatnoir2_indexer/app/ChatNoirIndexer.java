/*
 * Elasticsearch Indexer for WARC JSON Mapfiles using Hadoop MapReduce.
 * Copyright (C) 2014-2017 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package de.webis.chatnoir2.chatnoir2_indexer.app;

import de.webis.chatnoir2.chatnoir2_indexer.mapreduce.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.util.Arrays;

/**
 * Elasticsearch Indexer for WARC JSON corpora using Hadoop MapReduce.
 *
 * @author Janek Bevendorff
 */
public class ChatNoirIndexer extends Configured implements Tool
{
    private static final Logger LOG = Logger.getLogger(ChatNoirIndexer.class);

    private static final String[] SEQFILE_INPUT_OPTION     = { "sequence-files", "f" };
    private static final String[] UUID_PREFIX_INPUT_OPTION = { "uuid-prefix",    "u" };
    private static final String[] SPAMRANK_INPUT_OPTION    = { "spamranks",      "s" };
    private static final String[] PAGERANK_INPUT_OPTION    = { "pageranks",      "p" };
    private static final String[] ANCHOR_INPUT_OPTION      = { "anchortexts",    "a" };
    private static final String[] INDEX_INPUT_OPTION       = { "index",          "i" };
    private static final String[] INPUT_PARTITIONS_OPTION  = { "partitions",     "t" };
    private static final String[] INPUT_BATCHES_OPTION     = { "batches",        "n" };
    private static final String[] INPUT_BATCH_NUM_OPTION   = { "batch-num",      "b" };

    /**
     * Run this tool.
     */
    @SuppressWarnings("static-access")
    @Override
    public int run(final String[] args) throws Exception
    {
        final Options options = new Options();
        options.addOption(OptionBuilder.
                withArgName("NAME").
                hasArg().
                withLongOpt(INDEX_INPUT_OPTION[0]).
                withDescription("index name").
                isRequired().
                create(INDEX_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PREFIX").
                hasArg().
                withLongOpt(UUID_PREFIX_INPUT_OPTION[0]).
                withDescription("UUID prefix (e.g. clueweb12)").
                isRequired().
                create(UUID_PREFIX_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("GLOB").
                hasArg().
                withLongOpt(SEQFILE_INPUT_OPTION[0]).
                withDescription("directory containing input mapfiles").
                isRequired().
                create(SEQFILE_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(SPAMRANK_INPUT_OPTION[0]).
                withDescription("input path for spam ranks").
                isRequired(false).
                create(SPAMRANK_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(PAGERANK_INPUT_OPTION[0]).
                withDescription("input path for page ranks").
                isRequired(false).
                create(PAGERANK_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(ANCHOR_INPUT_OPTION[0]).
                withDescription("input path for anchor texts").
                isRequired(false).
                create(ANCHOR_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("NUM").
                hasArg().
                withLongOpt(INPUT_PARTITIONS_OPTION[0]).
                withDescription("number of input partitions (default: 100)").
                isRequired(false).
                create(INPUT_PARTITIONS_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("NUM").
                hasArg().
                withLongOpt(INPUT_BATCHES_OPTION[0]).
                withDescription("total number of batches (default: 1)").
                isRequired(false).
                create(INPUT_BATCHES_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("NUM").
                hasArg().
                withLongOpt(INPUT_BATCH_NUM_OPTION[0]).
                withDescription("which batch to run (default: 1)").
                isRequired(false).
                create(INPUT_BATCH_NUM_OPTION[1]));

        CommandLine cmdline;
        final CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().getSimpleName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        String indexName          = cmdline.getOptionValue(INDEX_INPUT_OPTION[0]);
        String seqFileInputPath   = cmdline.getOptionValue(SEQFILE_INPUT_OPTION[0]);
        String inputSpamRanks     = cmdline.getOptionValue(SPAMRANK_INPUT_OPTION[0]);
        String inputPageRanks     = cmdline.getOptionValue(PAGERANK_INPUT_OPTION[0]);
        String inputAnchors       = cmdline.getOptionValue(ANCHOR_INPUT_OPTION[0]);
        String uuidPrefix         = cmdline.getOptionValue(UUID_PREFIX_INPUT_OPTION[0]);
        String inputPartitionsStr = cmdline.getOptionValue(INPUT_PARTITIONS_OPTION[0]);
        String inputBatchesStr    = cmdline.getOptionValue(INPUT_BATCHES_OPTION[0]);
        String batchNumStr        = cmdline.getOptionValue(INPUT_BATCH_NUM_OPTION[0]);

        int inputPartitions = 100;
        if (null != inputPartitionsStr) {
            inputPartitions = Integer.parseInt(inputPartitionsStr);
        }
        int inputBatches = 1;
        if (null != inputBatchesStr) {
            inputBatches = Integer.parseInt(inputBatchesStr);
        }
        int batchNum = 0;
        if (null != batchNumStr) {
            batchNum = Math.max(0, Integer.parseInt(batchNumStr) - 1);
        }

        LOG.info("Tool name:        " + ChatNoirIndexer.class.getSimpleName());
        LOG.info(" - batch:         " + (batchNum + 1) + " of " + inputBatches);
        LOG.info(" - partitions:    " + inputPartitions);
        LOG.info(" - index:         " + indexName);
        LOG.info(" - spamranks:     " + (null != inputSpamRanks ? inputSpamRanks : "[none]"));
        LOG.info(" - pageranks:     " + (null != inputPageRanks ? inputPageRanks : "[none]"));
        LOG.info(" - anchors:       " + (null != inputAnchors   ? inputAnchors   : "[none]"));

        // configure Hadoop for Elasticsearch
        final Configuration conf = getConf();

        conf.setBoolean(MRJobConfig.MAP_SPECULATIVE,    false);
        conf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

        conf.set("es.resource",                conf.get("es.resource", String.format("%s/warcrecord", indexName)));
        conf.set("es.mapping.id",              "uuid");
        conf.set("es.mapping.exclude",         "uuid");
        conf.set("es.input.json",              "false");
        conf.set("es.index.auto.create",       conf.get("es.index.auto.create",       "yes"));
        conf.set("es.http.timeout",            conf.get("es.http.timeout",            "1m"));
        conf.set("es.http.retries",            conf.get("es.http.retries",            "5"));
        conf.set("es.batch.size.entries",      conf.get("es.batch.size.entries",      "5000"));
        conf.set("es.batch.size.bytes",        conf.get("es.batch.size.bytes",        "2mb"));
        conf.set("es.batch.write.retry.count", conf.get("es.batch.write.retry.count", "5"));
        conf.set("es.batch.write.retry.wait",  conf.get("es.batch.write.retry.wait",  "30s"));
        conf.set("es.batch.write.refresh",     conf.get("es.batch.write.refresh",     "false"));

        conf.set("webis.mapfile.uuid.prefix", uuidPrefix);

        final Job job = Job.getInstance(conf);
        job.setJobName(String.format("chatnoir2-indexer: %s, batch %d of %d", indexName , batchNum + 1, inputBatches));
        job.setJarByClass(ChatNoirIndexer.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(WarcReducer.class);

        // add input formats for input paths
        if (!seqFileInputPath.endsWith("/")) {
            seqFileInputPath += "/";
        }
        if (inputBatches == 1) {
            MultipleInputs.addInputPath(job, new Path(seqFileInputPath + "data-r-*/data"), SequenceFileInputFormat.class, WarcMapper.class);
            LOG.info(" - sequence file: " + seqFileInputPath + "data-r-*/data");
        } else {
            int numFiles = inputPartitions / inputBatches;
            for (int i = numFiles * batchNum; i < numFiles + numFiles * batchNum; ++i) {
                String mapFile = String.format("data-r-%05d/data", i);
                LOG.info(" - sequence file: " + mapFile);
                MultipleInputs.addInputPath(job, new Path(seqFileInputPath + mapFile), SequenceFileInputFormat.class, WarcMapper.class);
            }
        }

        if (null != inputSpamRanks)
            MultipleInputs.addInputPath(job, new Path(inputSpamRanks), TextInputFormat.class, WarcSpamRankMapper.class);
        if (null != inputPageRanks)
            MultipleInputs.addInputPath(job, new Path(inputPageRanks), TextInputFormat.class, WarcPageRankMapper.class);
        if (null != inputAnchors)
            MultipleInputs.addInputPath(job, new Path(inputAnchors), TextInputFormat.class, WarcAnchorMapper.class);

        job.waitForCompletion(true);

        final Counters counters        = job.getCounters();
        final long numDocs             = counters.findCounter(WarcMapReduceBase.RecordCounters.RECORDS).getValue();
        final long numSkippedTooLarge  = counters.findCounter(WarcMapReduceBase.RecordCounters.SKIPPED_RECORDS_TOO_LARGE).getValue();
        final long numSkippedParseErr  = counters.findCounter(WarcMapReduceBase.RecordCounters.SKIPPED_RECORDS_HTML_PARSE_ERROR).getValue();
        final long numGenerated        = counters.findCounter(WarcMapReduceBase.RecordCounters.GENERATED_DOCS).getValue();
        final long numEmptyContent     = counters.findCounter(WarcMapReduceBase.RecordCounters.NO_CONTENT).getValue();
        LOG.info(String.format("Read %d records total.", numDocs));
        LOG.info(String.format("Skipped %d oversized records.", numSkippedTooLarge));
        LOG.info(String.format("Skipped %d due to HTML parse errors.", numSkippedParseErr));
        LOG.info(String.format("Generated %d JSON documents.", numGenerated));
        LOG.info(String.format("Skipped %d documents due to no or empty plain-text content.", numEmptyContent));

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
     */
    public static void main(final String[] args) throws Exception
    {
        LOG.info("Running " + ChatNoirIndexer.class.getSimpleName() + " with args "
                + Arrays.toString(args));
        System.exit(ToolRunner.run(new ChatNoirIndexer(), args));
    }
}
