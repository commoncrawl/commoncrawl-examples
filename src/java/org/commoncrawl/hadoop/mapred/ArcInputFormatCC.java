package org.commoncrawl.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.hadoop.mapred.ArcRecordReaderCC;

/**
 * A input format the reads arc files.
 */
public class ArcInputFormatCC
  extends FileInputFormat<Text, Text> {

  /**
   * Returns the <code>RecordReader</code> for reading the arc file.
   * 
   * @param split The InputSplit of the arc file to process.
   * @param job The job configuration.
   * @param reporter The progress reporter.
   */
  public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(split.toString());
    return new ArcRecordReaderCC(job, (FileSplit)split);
  }

  /**
   * <p>Always returns false to indicate that ARC files are not splittable.</p>
   * <p>ARC files are stored in 100MB files.  Records are individually compressed
   * so they technically are splitable.  However, with more and more Hadoop clusters
   * moving to 128MB block sizes, we're not sure adding the complexity of making ARC
   * files splittable is warranted.  TDB.</p>
   */
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}


