package org.commoncrawl.examples;

// Java classes
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
import org.commoncrawl.util.shared.TextBytes;
import org.commoncrawl.util.shared.URLUtils;

// Google Gson classes
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

// Google Guava classes
import com.google.common.net.InternetDomainName;

/**
 * An example showing how to use the Common Crawl 'metadata' files to quickly
 * gather high level information about the corpus' content.
 * 
 * @author Chris Stephens <chris@commoncrawl.org>
 */
public class ExampleMetadataDomainPageCount extends Configured implements Tool {

  /**
   * Mapping class that produces the normalized domain name and a count of '1'
   * for every successfully retrieved URL in the Common Crawl corpus.
   */ 
  public static class ExampleMetadataDomainPageCountMapper
      extends    MapReduceBase
      implements Mapper<TextBytes, TextBytes, Text, LongWritable> {

    private static final Logger _logger = Logger.getLogger(ExampleMetadataDomainPageCount.class);

    // create a counter group for Mapper-specific statistics
    private final String _counterGroup = "Custom Mapper Counters";

    // implement the main "map" function
    public void map(TextBytes key, TextBytes value, OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {

      // key & value are "TextBytes" right now ...
      String url   = key.toString();
      String json  = value.toString();

      try {

        // Get the base domain name
        URI uri = new URI(url);
        String host = uri.getHost();

        if (host == null) {
          reporter.incrCounter(this._counterGroup, "Invalid URI", 1);
          return;
        }

        InternetDomainName domainObj = InternetDomainName.from(host);

        String domain = domainObj.topPrivateDomain().name();

        if (domain == null) {
          reporter.incrCounter(this._counterGroup, "Invalid Domain", 1);
          return;
        }

        // See if the page has a successful HTTP code
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObj    = jsonParser.parse(json).getAsJsonObject();

        int httpCode;

        if (jsonObj.has("http_result") == false) {
          reporter.incrCounter(this._counterGroup, "HTTP Code Missing", 1);
          return;
        }

        if (jsonObj.get("http_result").getAsInt() == 200) {
          reporter.incrCounter(this._counterGroup, "HTTP Success", 1);

          // only output counts for pages that were successfully retrieved
          output.collect(new Text(domain), new LongWritable(1));
        }
        else {
          reporter.incrCounter(this._counterGroup, "HTTP Not Success", 1);
        }
      }
      catch (IOException ex) {
        throw ex;
      }
      catch (Exception ex) {
        _logger.error("Caught Exception", ex); 
        reporter.incrCounter(this._counterGroup, "Exceptions", 1);
      }
    }
  }

  private static final Logger _logger = Logger.getLogger(ExampleMetadataDomainPageCount.class);

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

    conf.setJarByClass(ExampleMetadataDomainPageCount.class);

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
    String inputFile  = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/1331160113237/metadata-00009";
    String outputPath = "s3n://commoncrawl-dev/output/ExampleMetadataDomainPageCount";

    _logger.info("Reading metadata data from '"+ inputFile + "'");
    _logger.info("Writing output data to '"+ outputPath + "'");

    // Set which InputFormat class to use.
    conf.setInputFormat(SequenceFileInputFormat.class);

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
    conf.setMapperClass(ExampleMetadataDomainPageCount.ExampleMetadataDomainPageCountMapper.class);
    conf.setReducerClass(LongSumReducer.class);

    // Optionally, set other job parameters.
    //conf.setNumMapTasks(1);
    //conf.setNumReduceTasks(1);
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
    int res = ToolRunner.run(new Configuration(), new ExampleMetadataDomainPageCount(), args);
    System.exit(res);
  }
}
