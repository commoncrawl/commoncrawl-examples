package org.commoncrawl.examples;

// Java classes
import java.lang.Math;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

// Apache Project classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Common Crawl classes
import org.commoncrawl.hadoop.io.ArcInputFormat;
import org.commoncrawl.hadoop.io.ArcRecord;

// jsoup classes
import org.jsoup.Jsoup;

/**
 * An example showing how to use the Common Crawl 'metadata' files to quickly
 * gather high level information about the corpus' content.
 * 
 * @author Chris Stephens <chris@commoncrawl.org>
 */
public class ExampleArcWordCount extends Configured implements Tool {

  private static final Logger _logger = Logger.getLogger(ExampleArcWordCount.class);

  /**
   * Parse and output all words contained within the displayed text of each
   * page in the ARC files.
   */
  public static class ExampleArcWordCountMapper
      extends    MapReduceBase 
      implements Mapper<Text, ArcRecord, Text, LongWritable> {

    // create a counter group for Mapper-specific statistics
    private final String _counterGroup = "Custom Mapper Counters";

    public void map(Text key, ArcRecord value, OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {

      reporter.incrCounter(this._counterGroup, "Records In", 1);

      try {

        reporter.incrCounter(this._counterGroup, "Content Type - "+value.getContentType(), 1);

        // Ignore non-HTML content.
        if (!value.getContentType().equals("text/html")) {
          reporter.incrCounter(this._counterGroup, "Skipped - Non-HTML", 1);
          return;
        }

        // Let's pretend that the page content is ASCII ...
        String content = new String(value.getContent(), "US-ASCII");

        if (content == null || content == "") {
          reporter.incrCounter(this._counterGroup, "Skipped - Empty Content", 1);
        }

        // Parses HTML with a tolerant parser and extracts all text.
        String pageText = Jsoup.parse(content).text();

        // Removes all punctuation.
        pageText = pageText.replaceAll("[^a-zA-Z0-9 ]", "");

        // Normalizes whitespace to single spaces.
        pageText = pageText.replaceAll("\\s+", " ");

        if (pageText == null || pageText == "") {
          reporter.incrCounter(this._counterGroup, "Skipped - Empty Page Text", 1);
        }

        // Splits by space and outputs to OutputCollector.
        for (String word: pageText.split(" ")) {
          output.collect(new Text(word.toLowerCase()), new LongWritable(1));
        }
      }
      catch (Exception ex) {
        _logger.error("Caught Exception", ex);
        reporter.incrCounter(this._counterGroup, "Exceptions", 1);
      }
    }
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  @Override
  public int run(String[] args)
      throws Exception {

    // Creates a new job configuration for this Hadoop job.
    JobConf conf = new JobConf(this.getConf());

    conf.setJarByClass(ExampleArcWordCount.class);

    // Set your Amazon S3 credentials.
    BufferedReader in = new BufferedReader(new FileReader(System.getProperty("user.home") + File.separatorChar + ".awssecret"));
    String awsAccessKeyId     = in.readLine();
    String awsSecretAccessKey = in.readLine();
    in.close();

    conf.set("fs.s3.awsAccessKeyId",      awsAccessKeyId);
    conf.set("fs.s3.awsSecretAccessKey",  awsSecretAccessKey);

    conf.set("fs.s3n.awsAccessKeyId",     awsAccessKeyId);
    conf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey);

    // Define the input and output paths for this example
    String inputFile  = "s3n://aws-publicdatasets/common-crawl/crawl-002/2010/09/25/44/1285408080668_44.arc.gz";
    String outputPath = "s3n://commoncrawl-dev/output/ExampleArcWordCount";

    _logger.info("reading from '"+ inputFile + "'");
    _logger.info("writing to '"+ outputPath + "'");

    // Set which InputFormat class to use.
    conf.setInputFormat(ArcInputFormat.class);

    // Set the path where Hadoop should get the input data from.
    SequenceFileInputFormat.addInputPath(conf, new Path(inputFile));

    // Delete the output path directory if it already exists.
    FileSystem fs = FileSystem.get(new URI("s3n://commoncrawl-dev"), conf);

    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true);

    // Set which OutputFormat class to use.
    conf.setOutputFormat(TextOutputFormat.class);

    // Set the path where final output 'part' files will be saved.
    TextOutputFormat.setOutputPath(conf, new Path(outputPath));
    TextOutputFormat.setCompressOutput(conf, false);

    // Set the output data types.
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);

    // Set which Mapper and Reducer classes to use.
    conf.setMapperClass(ExampleArcWordCount.ExampleArcWordCountMapper.class);
    conf.setReducerClass(LongSumReducer.class);

    // Allow up to 10% of input records to fail mapping.
    conf.set("mapred.max.map.failures.percent", "10");

    // Optionally, set other job parameters.
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    //conf.setMaxMapAttempts(2);
    //conf.setMaxTaskFailuresPerTracker(1);

    // Run the job.
    RunningJob job = JobClient.runJob(conf);

    if (job.isSuccessful())
      return 0;
    else
      return 1;
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExampleArcWordCount(), args);
    System.exit(res);
  }
}
