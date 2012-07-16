package org.commoncrawl.hadoop.mapred;

import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.lang.Runtime;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.log4j.Logger;

import org.commoncrawl.hadoop.mapred.ArcRecord;

/**
 * <p>The <code>ArcRecordReader</code> class provides a record reader which 
 * reads records from arc files.</p>
 * 
 * <p>Arc files are essentially tars of gzips.  Each record in an arc file is
 * a compressed gzip.  Multiple records are concatenated together to form a
 * complete arc.  For more information on the arc file format see</p>
 * <p><a href="http://www.archive.org/web/researcher/ArcFileFormat.php">
 * http://www.archive.org/web/researcher/ArcFileFormat.php
 * </a></p>
 * 
 * <p>Arc files are used by the internet archive and grub projects.</p>
 * 
 * @see <a href="http://www.archive.org/">http://www.archive.org/</a>
 */
public class ArcRecordReader
    implements RecordReader<Text, ArcRecord> {

  private static final Logger LOG = Logger.getLogger(ArcRecordReader.class);

  private org.commoncrawl.nutch.tools.arc.ArcRecordReader _impl;

  /**
   * {@inheritDoc}
   */
  public ArcRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    this._impl = new org.commoncrawl.nutch.tools.arc.ArcRecordReader(conf, split);
  }

  /**
   * {@inheritDoc}
   */
  public void close()
    throws IOException {
    this._impl.close();
  }

  /**
   * {@inheritDoc}
   */
  public Text createKey() {
    return new Text();
  }

  /**
   * {@inheritDoc}
   */
  public ArcRecord createValue() {
    return new ArcRecord();
  }

  /**
   * {@inheritDoc}
   */
  public long getPos()
    throws IOException {
    return this._impl.getPos();
  }

  /**
   * {@inheritDoc}
   */
  public float getProgress()
    throws IOException {
    return this._impl.getProgress();
  }

  private final int _maxRecursion = 100;
  private       int _recursion    = 0;

  private boolean _callNext(Text key, ArcRecord value)
      throws IOException {
    boolean rv;
    this._recursion++;
    rv = this.next(key, value);
    this._recursion--;
    return rv;
  }

  /**
   * <p>Returns true if the next record in the split is read into the key and 
   * value pair.  The key will be the arc record header and the values will be
   * the raw content bytes of the arc record.</p>
   * 
   * @param key The record key
   * @param value The record value
   * 
   * @return True if the next record is read.
   * 
   * @throws IOException If an error occurs while reading the record value.
   */
  public boolean next(Text key, ArcRecord value)
      throws IOException {

    BytesWritable bytes = new BytesWritable();

    boolean rv;
    
    // get the next record from the underlying Nutch implementation
    rv = this._impl.next(key, bytes);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Entering RecordReader.next() - recursion = " + this._recursion);
      LOG.debug("- ARC Record Header (Nutch): [" + key.toString() + "]");
      LOG.debug("- ARC Record Content Size (Nutch):  " + String.format("%,12d", bytes.getLength()));
      LOG.debug("- Free / Curr JVM / Max JVM Memory: " + String.format("%,12d", Runtime.getRuntime().freeMemory()  / 1024 / 1024) + "MB "
                                                       + String.format("%,12d", Runtime.getRuntime().totalMemory() / 1024 / 1024) + "MB "
                                                       + String.format("%,12d", Runtime.getRuntime().maxMemory()   / 1024 / 1024) + "MB");
    }

		// if 'false' is returned, EOF has been reached
    if (rv == false) {
      if (LOG.isDebugEnabled())
        LOG.debug("Nutch ARC reader returned FALSE at " + this.getPos());
      return false;
    }

    // if we've found too many invalid records in a row, bail ...
    if (this._recursion > this._maxRecursion) {
      LOG.error("Found too many ("+this._maxRecursion+") invalid records in a row.  Aborting ...");
      return false;
    }

    // get the ARC record header returned from Nutch
    String arcRecordHeader = key.toString();

    // perform a basic sanity check on the record header
    if (arcRecordHeader.length() < 12) {
      LOG.error("Record at offset " + this.getPos() + " does not have appropriate ARC header: [" + arcRecordHeader + "]");
      return this._callNext(key, value);
    }

    // skip the ARC file header record
    if (arcRecordHeader.startsWith("filedesc://")) {
      LOG.info("File header detected: skipping record at " + this.getPos() + " [ " + arcRecordHeader + "]");
      return this._callNext(key, value);
    }

    try {

      // split ARC metadata into fields
      value.setArcRecordHeader(arcRecordHeader);

      if (LOG.isDebugEnabled())
        LOG.debug("- ARC Record Size (ARC Header):     " + String.format("%,12d", value.getContentLength()));

      // set the key to the URL
      key.set(value.getURL());

      // see if we need to parse HTTP headers
      if (arcRecordHeader.startsWith("http://")) {
        value.setParseHttpMessage(true);
      }

      // set the content, and parse the headers (if necessary)
      value.setContent(bytes);
    }
    catch (IllegalArgumentException ex) {
      LOG.error("Unable to process record at offset " + this.getPos() + ": ", ex);
      return this._callNext(key, value);
    }
    catch (ParseException ex) {
      LOG.error("Unable to process record at offset " + this.getPos() + ": ", ex);
      return this._callNext(key, value);
    }

    return true;
  }
}
