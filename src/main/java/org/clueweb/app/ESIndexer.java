/*
 * Elasticsearch Indexer for ClueWeb09/12 using Hadoop MapReduce.
 * Based on ClueWeb Tools <https://github.com/lintool/clueweb>
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

package org.clueweb.app;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.clueweb.mapreduce.*;
import org.clueweb.clueweb09.mapreduce.ClueWeb09InputFormat;
import org.clueweb.clueweb12.mapreduce.ClueWeb12InputFormat;

import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.util.Arrays;
import java.util.UUID;

/**
 * Elasticsearch Indexer for ClueWeb09/12 using Hadoop MapReduce.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ESIndexer extends Configured implements Tool
{
    private static final Logger LOG = Logger.getLogger(ESIndexer.class);

    public static final String[] CLUEWEB_VERSION_INPUT_OPTION  = { "version",     "v" };
    public static final String[] WARC_INPUT_OPTION             = { "warcs",       "w" };
    public static final String[] SPAMRANK_INPUT_OPTION         = { "spamranks",   "s" };
    public static final String[] PAGERANK_INPUT_OPTION         = { "pageranks",   "p" };
    public static final String[] ANCHOR_INPUT_OPTION           = { "anchortexts", "a" };
    public static final String[] ANCHOR_INDEX_OPTION           = { "index",       "i" };

    /**
     * Run this tool.
     */
    @Override @SuppressWarnings("static-access")
    public int run(String[] args) throws Exception
    {
        final Options options = new Options();
        options.addOption(OptionBuilder.
                withArgName("09 | 12").
                hasArg().
                withLongOpt(CLUEWEB_VERSION_INPUT_OPTION[0]).
                withDescription("ClueWeb version").
                isRequired().
                create(CLUEWEB_VERSION_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(WARC_INPUT_OPTION[0]).
                withDescription("input path for WARC records").
                isRequired().
                create(WARC_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(SPAMRANK_INPUT_OPTION[0]).
                withDescription("input path for spam ranks").
                isRequired().
                create(SPAMRANK_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(PAGERANK_INPUT_OPTION[0]).
                withDescription("input path for page ranks").
                isRequired().
                create(PAGERANK_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(ANCHOR_INPUT_OPTION[0]).
                withDescription("input path for anchor texts").
                isRequired().
                create(ANCHOR_INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("NAME").
                hasArg().
                withLongOpt(ANCHOR_INDEX_OPTION[0]).
                withDescription("index name (default: clueweb[VERSION])").
                isRequired(false).
                create(ANCHOR_INDEX_OPTION[1]));

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

        // clueweb version and input paths
        final String clueWebVersion = cmdline.getOptionValue(CLUEWEB_VERSION_INPUT_OPTION[0]);
        final String inputWarc      = cmdline.getOptionValue(WARC_INPUT_OPTION[0]);
        final String inputSpamRanks = cmdline.getOptionValue(SPAMRANK_INPUT_OPTION[0]);
        final String inputPageRanks = cmdline.getOptionValue(PAGERANK_INPUT_OPTION[0]);
        final String inputAnchors   = cmdline.getOptionValue(ANCHOR_INPUT_OPTION[0]);
        final String indexName      = null != cmdline.getOptionValue(ANCHOR_INDEX_OPTION[0]) ?
                cmdline.getOptionValue(ANCHOR_INDEX_OPTION[0]) : String.format("clueweb%s", clueWebVersion);

        if (!clueWebVersion.equals("09") && !clueWebVersion.equals("12")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().getSimpleName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.err.println("Argument error: ClueWeb version must be either 09 or 12.");
            return -1;
        }

        LOG.info("Tool name: " + ESIndexer.class.getSimpleName());
        LOG.info(" - input: "  + inputWarc);

        // configure Hadoop for Elasticsearch
        final Configuration conf = getConf();

        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);

        conf.set("es.nodes",             conf.get("es.nodes", "betaweb020.medien.uni-weimar.de:9200"));
        conf.set("es.resource",          conf.get("es.resource", String.format("%s/page", indexName)));
        conf.set("es.input.json",        "no");
        conf.set("es.index.auto.create", "yes");
        conf.set("es.batch.size.bytes",  "50mb");

        final Job job = Job.getInstance(conf);
        job.setJobName(String.format("clueweb%s-esindex-%s", clueWebVersion, UUID.randomUUID()));
        job.setJarByClass(ESIndexer.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(CluewebMapReducer.class);

        // add input formats for input paths
        if (clueWebVersion.equals("09")) {
            MultipleInputs.addInputPath(job, new Path(inputWarc), ClueWeb09InputFormat.class, ClueWebWarcMapper.class);
        } else {
            MultipleInputs.addInputPath(job, new Path(inputWarc), ClueWeb12InputFormat.class, ClueWebWarcMapper.class);
        }
        MultipleInputs.addInputPath(job, new Path(inputSpamRanks), TextInputFormat.class, ClueWebSpamRankMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPageRanks), TextInputFormat.class, ClueWebPageRankMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputAnchors),   TextInputFormat.class, ClueWebAnchorMapper.class);

        job.waitForCompletion(true);

        final Counters counters = job.getCounters();
        int numDocs = (int)counters.findCounter(ClueWebWarcMapper.Records.PAGES).getValue();
        LOG.info("Read " + numDocs + " records.");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
     */
    public static void main(String[] args) throws Exception
    {
        LOG.info("Running " + ESIndexer.class.getSimpleName() + " with args "
                + Arrays.toString(args));
        System.exit(ToolRunner.run(new ESIndexer(), args));
    }
}
