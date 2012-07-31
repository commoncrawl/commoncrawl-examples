package org.commoncrawl.hadoop.mapred;

import java.io.BufferedReader;
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

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicLineParser;
import org.apache.http.params.BasicHttpParams;

import org.commoncrawl.compressors.gzip.GzipCompressorInputStream;

/**
 * Reads ARC records.
 * 
 * Set "io.file.buffer.size" to define the amount of data that should be
 * buffered from S3.
 */
public class ArcRecordReaderCC
    implements RecordReader<Text, Text> {

  private static final Logger LOG = Logger.getLogger(ArcRecordReaderCC.class);

  private FSDataInputStream         _fsin;
  private GzipCompressorInputStream _gzip;
  private long                      _fileLength;

  private StringBuffer _line;

  private byte[] _buffer;
  private int    _bufferSize   = 4096;
  private int    _bufferLength = 0;

  private String[] _fieldNames;

  /**
   *
   */
  public ArcRecordReaderCC(Configuration job, FileSplit split)
      throws IOException { 

    if (split.getStart() != 0) {
      LOG.error("ARC files are not splittable");
      throw new IOException("Error");
    }

    // TO-DO: Consider using a different parameter to determine buffer size in
    //        this class.  Make buffer at least 1024 bytes.
    this._bufferSize = Math.max(job.getInt("io.file.buffer.size", this._bufferSize), 1024);
    this._bufferLength = 0;

    this._buffer = new byte[this._bufferSize];
    this._line   = new StringBuffer(this._bufferSize);

    // open the file and seek to the start of the split
    final Path file = split.getPath();

    FileSystem fs = file.getFileSystem(job);

    LOG.info("Split Length:    " + split.getLength());
    LOG.info("ARC File Length: " + fs.getFileStatus(file).getLen());

    this._fsin = fs.open(file);

    // create a GZIP stream that *does not* automatically read through members
    this._gzip = new GzipCompressorInputStream(this._fsin, false);

    this._fileLength = fs.getFileStatus(file).getLen();

    // read the ARC file header.  determine if the file is valid.
    boolean isValid = this._readArcFileHeader();

    if (!isValid) 
      throw new IOException("Invalid ARC file format");
  }

  /**
   *  
   */
  private String _readArcRecordHeader()
      throws IOException {

    // read a line of content
    int b = this._gzip.read();
    int n = 1;

    // if -1 is returned, we are at EOF
    if (b == -1)
      return null;

    this._line.setLength(0);

    // read until an NL
    do {

      if (((char) b) == '\n')
        break;

      this._line.append((char) b);

      b = this._gzip.read();
      n++;
    }
    while (b != -1);

    return this._line.toString();

    // if (values.length == this._fieldNames.length)
    //   return values;
  }

  /**
   * Parses the ARC file header.
   *
   * @return TRUE if the ARC file header is valid.  FALSE if not.
   */
  private boolean _readArcFileHeader()
      throws IOException {

    // This only happens once per file - choose clarity and forgiveness
    // over absolute efficiency.
    BufferedReader reader = new BufferedReader(new InputStreamReader(this._gzip, "US-ASCII"));

    String line;

    // example: filedesc://1341709173972_1004.arc.gz 0.0.0.0 20120708005942 text/plain 73
    if ((line = reader.readLine()) == null) {
      LOG.error("End of input found before completed ARC file header.");
      return false;
    }

    if (!line.trim().toLowerCase().startsWith("filedesc://")) {
      LOG.error("ARC file header does not start with 'filedesc' declaration.");
      return false;
    }

    // example: 1 0 CommonCrawl
    if ((line = reader.readLine()) == null) {
      LOG.error("End of input found before completed ARC file header.");
      return false;
    }

    // example: URL IP-address Archive-date Content-type Archive-length
    if ((line = reader.readLine()) == null) {
      LOG.error("End of input found before completed ARC file header.");
      return false;
    }

    this._fieldNames = line.split(" ");

    int i;

    // clean up each field name
    for (i = 0; i < this._fieldNames.length; i++) {
      this._fieldNames[i] = this._fieldNames[i].trim().toUpperCase().replaceAll("[^A-Z0-9]", "");
      LOG.info("ARC Record Header Field: " + this._fieldNames[i]);
    }

    // skip the next blank line
    reader.readLine();

    // skip lines until EOI is reached
    for (i = 0; i < 5; i++) {
      if (reader.readLine() == null)
        break;
    }

    if (i >= 5) {
      LOG.error("Too many extra lines found at the end of the ARC file header.");
      return false;
    }
    else if (i >  1) {
      LOG.warn("Extra lines found at the end of the ARC file header.");
    }

    this._gzip.nextMember();

    return true;
  }

  /**
   *
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
  public Text createValue() {
    return new Text();
  }

  /**
   * 
   */
  public synchronized boolean next(Text key, Text value)
      throws IOException {

    String recordHeader = this._readArcRecordHeader();

    // on EOF, return FALSE
    if (recordHeader == null)
      return false;

    // parse the ARC record header
    String[] fields = recordHeader.split(" ");

    if (fields.length != 5) {
      value.set("[invalid]"); this._skipRecord();
      return true;
    } 

    if (!fields[0].toLowerCase().trim().startsWith("http://")) {
      value.set("[invalid]"); this._skipRecord();
      return true;
    } 

    int contentLength = 1;

    try {
      contentLength = Integer.parseInt(fields[4]);
    }
    catch (Exception ex) {
      value.set("[invalid]"); this._skipRecord();
      return true;
    }

    byte[] content = new byte[contentLength];
    int n = 0;

    LOG.info("Content Length:  " + contentLength);

    n = this._gzip.read(content, 0, contentLength);

    LOG.info("Content Read:    " + n);

    value.set("[valid]");

    n = this._gzip.read(this._buffer, 0, this._bufferSize);

    LOG.info("Additional Bytes: " + n);

    this._gzip.nextMember();

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
