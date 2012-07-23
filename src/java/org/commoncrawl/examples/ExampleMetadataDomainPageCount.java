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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
public class ExampleMetadataDomainPageCount
    extends    Configured
    implements Tool {

  private static final Logger LOG = Logger.getLogger(ExampleMetadataDomainPageCount.class);

  /**
   * Mapping class that produces the normalized domain name and a count of '1'
   * for every successfully retrieved URL in the Common Crawl corpus.
   */ 
  public static class ExampleMetadataDomainPageCountMapper
      extends    MapReduceBase
      implements Mapper<Text, Text, Text, LongWritable> {

    // create a counter group for Mapper-specific statistics
    private final String _counterGroup = "Custom Mapper Counters";

    // implement the main "map" function
    public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {

      // key & value are "Text" right now ...
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
        LOG.error("Caught Exception", ex); 
        reporter.incrCounter(this._counterGroup, "Exceptions", 1);
      }
    }
  }


  /**
   * Hadoop FileSystem PathFilter for ARC files, allowing users to limit the
   * number of files processed.
   *
   * @author Chris Stephens <chris@commoncrawl.org>
   */
  public static class SampleFilter
      implements PathFilter {

    private static int count =         0;
    private static int max   = 999999999;

    public boolean accept(Path path) {

      if (!path.getName().startsWith("metadata-"))
        return false;

      SampleFilter.count++;

      if (SampleFilter.count > SampleFilter.max)
        return false;

      return true;
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

    String outputPath = null;
    String configFile = null;

    // Read the command line arguments.
    if (args.length <  1)
      throw new IllegalArgumentException("Example JAR must be passed an output path.");

    outputPath = args[0];

    if (args.length >= 2)
      configFile = args[1];

    // For this example, only look at a single metadata file.
    String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/1341690166822/metadata-01849";
 
    // Switch to this if you'd like to look at all metadata files.  May take many minutes just to read the file listing.
    // String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/*/metadata-*";

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Creates a new job configuration for this Hadoop job.
    JobConf job = new JobConf(this.getConf());

    job.setJarByClass(ExampleMetadataDomainPageCount.class);

    // Scan the provided input path for ARC files.
    LOG.info("setting input path to '"+ inputPath + "'");
    FileInputFormat.addInputPath(job, new Path(inputPath));

    // Optionally, you can add in a custom input path filter
    // FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    // Delete the output path directory if it already exists.
    LOG.info("clearing the output path at '" + outputPath + "'");

    FileSystem fs = FileSystem.get(new URI(outputPath), job);

    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true);

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    job.setInputFormat(SequenceFileInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormat(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(ExampleMetadataDomainPageCount.ExampleMetadataDomainPageCountMapper.class);
    job.setReducerClass(LongSumReducer.class);

    if (JobClient.runJob(job).isSuccessful())
      return 0;
    else
      return 1;
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExampleMetadataDomainPageCount(), args);
    System.exit(res);
  }
}

