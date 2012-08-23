package org.commoncrawl.hadoop.mapred;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.lang.Math;
import java.lang.StringBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.log4j.Logger;

import org.commoncrawl.compressors.gzip.GzipCompressorInputStream;

/**
 * Reads ARC records.
 * 
 * Set "io.file.buffer.size" to define the amount of data that should be
 * buffered from S3.
 */
public class ArcRecordReader
    implements RecordReader<Text, ArcRecord> {

  private static final Logger LOG = Logger.getLogger(ArcRecordReader.class);

  private FSDataInputStream         _fsin;
  private GzipCompressorInputStream _gzip;
  private long                      _fileLength;

  /**
   *
   */
  public ArcRecordReader(Configuration job, FileSplit split)
      throws IOException { 

    if (split.getStart() != 0) {
      IOException ex = new IOException("Invalid ARC file split start " + split.getStart() + ": ARC files are not splittable");
      LOG.error(ex.getMessage());
      throw ex; 
    }

    // open the file and seek to the start of the split
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(job);

    this._fsin = fs.open(file);

    // create a GZIP stream that *does not* automatically read through members
    this._gzip = new GzipCompressorInputStream(this._fsin, false);

    this._fileLength = fs.getFileStatus(file).getLen();

    // First record should be an ARC file header record.  Skip it.
    this._skipRecord();
  }

  /**
   * Skips the current record, and advances to the next GZIP member.
   */
  private void _skipRecord()
      throws IOException {

    long n = 0;

    do {
      n = this._gzip.skip(999999999);
    }
    while (n > 0);

    this._gzip.nextMember();
  }
  
  /**
   * @inheritDoc
   */
  public Text createKey() {
    return new Text();
  }
  
  /**
   * @inheritDoc
   */
  public ArcRecord createValue() {
    return new ArcRecord();
  }

  private static byte[] _checkBuffer = new byte[64];

  /**
   * 
   */
  public synchronized boolean next(Text key, ArcRecord value)
      throws IOException {

    boolean isValid = true;
    
    // try reading an ARC record from the stream
    try {
      isValid = value.readFrom(this._gzip);
    }
    catch (EOFException ex) {
      return false;
    }

    // if the record is not valid, skip it
    if (isValid == false) {
      LOG.error("Invalid ARC record found at GZIP position "+this._gzip.getBytesRead()+".  Skipping ...");
      this._skipRecord();
      return true;
    }

    if (value.getURL() != null)
      key.set(value.getURL());

    // check to make sure we've reached the end of the GZIP member
    int n = this._gzip.read(_checkBuffer, 0, 64);

    if (n != -1) {
      LOG.error(n+"  bytes of unexpected content found at end of ARC record.  Skipping ...");
      this._skipRecord();
    }
    else {
      this._gzip.nextMember();
    }
   
    return true;
  }

  /**
   * @inheritDoc
   */
  public float getProgress()
      throws IOException {
    return Math.min(1.0f, this._gzip.getBytesRead() / (float) this._fileLength);
  }
  
  /**
   * @inheritDoc
   */
  public synchronized long getPos()
      throws IOException {
    return this._gzip.getBytesRead();
  }

  /**
   * @inheritDoc
   */
  public synchronized void close()
      throws IOException {

    if (this._gzip != null)
      this._gzip.close(); 
  }

}
