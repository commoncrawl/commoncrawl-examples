package org.commoncrawl.examples;

// Java classes
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

// Apache Project classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
public class ExampleMetadataStats
    extends    Configured
    implements Tool {

  private static final Logger LOG = Logger.getLogger(ExampleMetadataStats.class);

  /**
   * Mapping class that produces statistics about the Common Crawl corpus.
   */ 
  public static class ExampleMetadataStatsMapper
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
 
        // See if the page has a successful HTTP code
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObj    = jsonParser.parse(json).getAsJsonObject();

        boolean isSuccessful = false;

        String disposition = "[no status]";
 
        if (jsonObj.has("disposition"))
        {
          disposition = jsonObj.get("disposition").getAsString().trim().toUpperCase();

          if (disposition.equals("SUCCESS"))
            isSuccessful = true;
        }

        // Output a basic page count
        output.collect(new Text("Pages Requested\tTotal"), new LongWritable(1));

        output.collect(new Text("Pages Requested\t"+disposition), new LongWritable(1));

        // Output the HTTP result
        String httpResult = "[missing]";

        if (jsonObj.has("http_result"))
          httpResult = jsonObj.get("http_result").getAsString().trim().toUpperCase();

        output.collect(new Text("HTTP Code\t"+httpResult+" ("+disposition+")"), new LongWritable(1));

        // If the request was not successful, move to the next record
        if (isSuccessful == false)
          return;

        // Gather the host name
        try {

          URI uri = new URI(url);
          String host = uri.getHost();

          if (host == null || host.equals(""))
            throw new URISyntaxException(url, "Unable to gather host or no host found");

          // Gather the domain object
          InternetDomainName domainObj = InternetDomainName.from(host);

          // Output the TLD
          String publicSuffix = "[none]";

          if (domainObj.hasPublicSuffix())
            publicSuffix = domainObj.publicSuffix().name().trim().toLowerCase();

          output.collect(new Text("TLD\t"+publicSuffix), new LongWritable(1));

          // Output the private domain
          // WARNING - This dramatically increases the size of the output.
          String privateDomain = "[invalid]";

          if (domainObj.topPrivateDomain() != null)
            privateDomain = domainObj.topPrivateDomain().name().trim().toLowerCase();

          //output.collect(new Text("Domain\t"+privateDomain), new LongWritable(1));
        }
        catch (URISyntaxException ex) {
          output.collect(new Text("TLD\t[invalid URL]"), new LongWritable(1));
          reporter.incrCounter(this._counterGroup, "Invalid URLs", 1);
        }
 
        // Output MIME Type
        String mimeType = "[missing]";

        if (jsonObj.has("mime_type"))
          mimeType = jsonObj.get("mime_type").getAsString().trim().toLowerCase();

        output.collect(new Text("Type\t"+mimeType), new LongWritable(1));

        // Output Charset
        String charset = "[missing]";

        if (jsonObj.has("charset_detected"))
          charset = jsonObj.get("charset_detected").getAsString().trim().toUpperCase();

        output.collect(new Text("Charset\t"+charset), new LongWritable(1));

        // Download Size
        if (jsonObj.has("download_size") == true)
          output.collect(new Text("Content Size\t"), new LongWritable(jsonObj.get("download_size").getAsInt()));
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
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  @Override
  public int run(String[] args)
      throws Exception {

    String baseInputPath = null;
    String outputPath    = null;

    // Read the command line arguments.
    if (args.length < 1)
      throw new IllegalArgumentException("'run()' must be passed an output path.");

    outputPath = args[0];

    // Creates a new job configuration for this Hadoop job.
    JobConf job = new JobConf(this.getConf());

    job.setJarByClass(ExampleMetadataStats.class);

    baseInputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment";

    FileSystem fs = null;

    // If you would like to process all segments, comment this out and
    // uncomment the block of code below
    String inputPath = baseInputPath + "/1346823845675/metadata-04379";

    LOG.info("adding input path '" + inputPath + "'");
    FileInputFormat.addInputPath(job, new Path(inputPath));
    /*
    fs = FileSystem.get(new URI("s3n://aws-publicdatasets"), job);

    for (FileStatus fileStatus : fs.globStatus(new Path("/common-crawl/parse-output/valid_segments/[0-9]*"))) { 
      String[] parts = fileStatus.getPath().toString().split("/");
      String inputPath = baseInputPath + "/" + parts[parts.length-1] + "/metadata-*";
      LOG.info("adding input path '" + inputPath + "'");
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }
    */

    // Delete the output path directory if it already exists.
    LOG.info("clearing the output path at '" + outputPath + "'");

    fs = FileSystem.get(new URI(outputPath), job);

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
    job.setMapperClass(ExampleMetadataStats.ExampleMetadataStatsMapper.class);
    job.setCombinerClass(LongSumReducer.class);
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

    int res = ToolRunner.run(new Configuration(), new ExampleMetadataStats(), args);
    System.exit(res);
  }
}

