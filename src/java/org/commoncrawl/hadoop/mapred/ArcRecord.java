package org.commoncrawl.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.lang.Character;
import java.lang.IllegalArgumentException;
import java.lang.Integer;
import java.lang.Math;
import java.lang.NumberFormatException;
import java.lang.StringBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;

// Hadoop classes
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

// Apache HTTP Components classes
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

// Apache log4j classes
import org.apache.log4j.Logger;

/**
 * An entry in an ARC (Internet Archive) data file.
 *
 * @author Chris Stephens
 */
public class ArcRecord
    implements Writable {

  private static final Logger LOG = Logger.getLogger(ArcRecord.class);

  // boolean indicating whether HTTP message should be parsed.
  private boolean _parseHttpMessage;

  // header & content
  private String        _arcRecordHeader;
  private BytesWritable _content;

  // ARC v1 metadata
  private String _url;
  private String _ipAddress;
  private Date   _archiveDate;
  private String _contentType;
  private int    _contentLength;

  // ARC v2 metadata
//private int    resultCode;
//private String checksum;
//private String location;
//private long   offset;
//private String filename;

  // HTTP Status Code
  private int _httpStatusCode;

  // HTTP headers
  private ArrayList<Header> _httpHeaders;

  /**
   * <p>Creates an empty ARC record.</p>
   */
  public ArcRecord() {

    this._parseHttpMessage = false;

    this._arcRecordHeader  = "";
    this._content          = new BytesWritable();

    this._url              = "";
    this._ipAddress        = "";
    this._archiveDate      = new Date(2000, 1, 1);
    this._contentType      = "";

    this._contentLength    = this._content.getLength();

    this._httpStatusCode   = 0;
    this._httpHeaders      = new ArrayList<Header>(10);
  }

  /**
   * <p>Creates a new ARC record with ARC record header data parsed and content
   * parsed and/or assigned.</p>.<p>Internally, simply calls <code>setArcRecordHeader()</code>
   * and <code>setContent()</code>.
   *
   * @param arcRecordHeader The first line of an ARC file entry - the header
   * line for an ARC file item.
   * @param content The body of the ARC file entry (including HTTP headers, if applicable).
   */
  public ArcRecord(String arcRecordHeader, BytesWritable content)
      throws IllegalArgumentException, ParseException, IOException {
    this.setArcRecordHeader(arcRecordHeader);
    this.setContent(content);
  }

  /**
   * <p>Parses and sets the ARC record header fields.</p>
   * <p>Currently, this method expects the ARC record header string to contain
   * the following fields, in order, separated by space:
   * <ul>
   * <li>URL</li>
   * <li>IP Address</li>
   * <li>Archive Date</li>
   * <li>Content Type</li>
   * <li>Content Length</li>
   * </ul>
   * </p>
   * <p>For more information on the arc file format, see
   * {@link http://www.archive.org/web/researcher/ArcFileFormat.php}.</p>
   *
   * @param arcRecordHeader The first line of an ARC file entry - the header
   *                        line for an ARC file item.
   */
  public void setArcRecordHeader(String arcRecordHeader)
      throws IllegalArgumentException, ParseException {

    if (arcRecordHeader == null || arcRecordHeader.equals(""))
      throw new IllegalArgumentException("ARC v1 record header string is empty.");

    String[] metadata = arcRecordHeader.split(" ");

    if (metadata.length != 5)
      throw new IllegalArgumentException("ARC v1 record header must be 5 fields.");

    if (arcRecordHeader.startsWith("http://"))
      this._parseHttpMessage = true;

    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    this._url            = metadata[0];
    this._ipAddress      = metadata[1];
    this._archiveDate    = format.parse(metadata[2]);
    this._contentType    = metadata[3];
    this._contentLength  = (new Integer(metadata[4])).intValue();
  }

  /**
   * <p>Sets the ARC record body content.</p>
   * <p>If HTTP message parsing is turned on, this function parses the HTTP envelope.</p>
   *
   * @param content The body of the ARC file entry (including HTTP status & 
   *                headers, if applicable).  This argument cannot be NULL.
   */
  public void setContent(BytesWritable content)
      throws IllegalArgumentException, ParseException, IOException {

    if (content == null)
      throw new IllegalArgumentException("ArcRecord cannot be created with NULL/missing content.");

    if (this._parseHttpMessage == false) {
      this._content = content;
    }
    else {

      // HTTP header parsing code shamelessly stolen from nutch
			ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes(), 0, content.getLength());

      StringBuffer buffer = new StringBuffer();
      PushbackInputStream pis = new PushbackInputStream(is);

      // read the status code
      this._httpStatusCode = this._parseStatusLine(pis, buffer);

      // read the headers
      this._parseHeaders(pis, buffer);

      // after headers are parsed, read the reset of the body content
      byte[] bytes = new byte[pis.available()];
      pis.read(bytes, 0, pis.available());

      this._content = new BytesWritable(bytes);
    }

    if (LOG.isDebugEnabled()) {
      if (this._content == null)
        LOG.debug("      - ARC content buffer is NULL");
      else
        LOG.debug("      - ARC content buffer capactiy:       " + String.format("%,12d", this._content.getCapacity()) + " bytes");
    }
  }

  /**
   * <p>Sets whether the HTTP message should be parsed out of the content.</p>
   *
   * @param parseHttpMessage Should be set to TRUE if the content contains an HTTP
   *                         envelope that needs to be parsed.
   */
  public void setParseHttpMessage(boolean parseHttpMessage) {
    this._parseHttpMessage = parseHttpMessage;
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    return this._url + " - " + this._archiveDate.toString() + " - " + this._contentType;
  }

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out)
      throws IOException {

    // write out ARC header info
    out.writeUTF(this._url);
    out.writeUTF(this._ipAddress);
    out.writeUTF(this._contentType);
    out.writeLong(this._archiveDate.getTime());
    out.writeInt(this._contentLength);

    // write out HTTP info
    out.writeInt(this._httpStatusCode);

    out.writeInt(this._httpHeaders.size());
    for (Header header : this._httpHeaders) {
      out.writeUTF(header.getName());
      out.writeUTF(header.getValue());
    }
 
    // write out content
    this._content.write(out);
  }

  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in)
      throws IOException {

    // read in ARC header info
    this._url           = in.readUTF();
    this._ipAddress     = in.readUTF();
    this._contentType   = in.readUTF();
    this._archiveDate   = new Date(in.readLong());
    this._contentLength = in.readInt();

    // read in HTTP info
    this._httpStatusCode = in.readInt();

    int size = in.readInt();
    this._httpHeaders = new ArrayList<Header>(size);
    for (int i = 0; i < size; i++) {
      String name  = in.readUTF();
      String value = in.readUTF();
      this._httpHeaders.add(new BasicHeader(name, value));
    }

    // read in content
    this._content.readFields(in);
  }

  /**
   * <p>Returns the ARC record content.</p>
   * <p>If HTTP headers were parsed, content is returned <i>without</i> HTTP headers,
   * and HTTP headers can be retrieved with <code>getHttpHeaders()</code>.
   * <p>Note: Data should *never* be written directly to this array.  Always use
   * <code>setContent()</code> to assign content to the ARC record.</p>
   *
   * @return The raw ARC record content data.
   */
  public byte[] getContent() {
    return this._content.getBytes();
  }

  /**
   * <p>Returns the URL from the ARC record header.</p>
   *
   * @return The URL for this entry.
   */
  public String getURL() {
    return this._url;
  }

  /**
   * <p>Returns the IP address from the ARC record header.</p>
   *
   * @return The IP address for this entry.
   */
  public String getIpAddress() {
    return this._ipAddress;
  }

  /**
   * <p>Returns the archive date from the ARC record header.</p>
   *
   * @return The archive date for this entry.
   */
  public Date getArchiveDate() {
    return this._archiveDate;
  }

  /**
   * <p>Returns the MIME content type from the ARC record header.</p>
   * <p>Note: The MIME content type in the ARC record header is not necessarily the
   * same as the <code>Content-Type</code> HTTP header inside the content body 
   * (if one is present).</p>
   *
   * @return The MIME content type for this entry.
   */
  public String getContentType() {
    return this._contentType;
  }

  /**
   * <p>Returns the content length from the ARC record header.</p>
   * <p>Note: The content length in the ARC record header is not necessarily the
   * same as the <code>Content-Length</code> HTTP header inside the content body 
   * (if one is present).</p>
   *
   * @return The content length for this entry.
   */
  public int getContentLength() {
    return this._contentLength;
  }

  /**
   * <p>Returns the HTTP status code, if HTTP headers were parsed out of the content.</p>
   *
   * @return The HTTP status code.
   */
  public int getHttpStatusCode() {
    return this._httpStatusCode;
  }

  /**
   * <p>Returns an array of HTTP headers, if HTTP headers were parsed out of the content.</p>
   *
   * @return An array of HTTP headers.
   */
  public Header[] getHttpHeaders() {
    return this._httpHeaders.toArray(new Header[this._httpHeaders.size()]);
  }

 
  // shamelessly forklifted from nutch
  private int _parseStatusLine(PushbackInputStream in, StringBuffer line)
      throws IOException, ParseException {
    _readLine(in, line, false);

    int codeStart = line.indexOf(" ");
    int codeEnd = line.indexOf(" ", codeStart+1);

    // handle lines with no plaintext result code, ie:
    // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
    if (codeEnd == -1)
      codeEnd= line.length();

    int code;
    try {
      code= Integer.parseInt(line.substring(codeStart+1, codeEnd));
    } catch (NumberFormatException e) {
      throw new ParseException("bad status line '" + line + "': " + e.getMessage(), 0);
    }

    return code;
  }

  // shamelessly forklifted from nutch
  private void _parseHeaders(PushbackInputStream in, StringBuffer line)
    throws IOException, ParseException {

    while (_readLine(in, line, true) != 0) {

      // handle HTTP responses with missing blank line after headers
      int pos;
      if ( ((pos= line.indexOf("<!DOCTYPE")) != -1) 
           || ((pos= line.indexOf("<?xml")) != -1) 
           || ((pos= line.indexOf("<HTML")) != -1) 
           || ((pos= line.indexOf("<html")) != -1) ) {

        in.unread(line.substring(pos).getBytes("US-ASCII"));
        line.setLength(pos);

        _processHeaderLine(line);

        return;
      }

      _processHeaderLine(line);
    }
  }

  // shamelessly forklifted from nutch
  private void _processHeaderLine(StringBuffer line)
    throws IOException, ParseException {

    int colonIndex = line.indexOf(":");       // key is up to colon

    if (colonIndex == -1) {
      int i;
      for (i= 0; i < line.length(); i++)
        if (!Character.isWhitespace(line.charAt(i)))
          break;
      if (i == line.length())
        return;
      throw new ParseException("No colon in HTTP header: " + line, 0);
    }
    String key = line.substring(0, colonIndex);

    int valueStart = colonIndex+1;            // skip whitespace
    while (valueStart < line.length()) {
      int c = line.charAt(valueStart);
      if (c != ' ' && c != '\t')
        break;
      valueStart++;
    }

    String value = line.substring(valueStart);

    this._httpHeaders.add(new BasicHeader(key, value));
  }

  // shamelessly forklifted from nutch
  private static int _readLine(PushbackInputStream in, StringBuffer line, boolean allowContinuedLine)
      throws IOException, ParseException {
    line.setLength(0);

    if (_peek(in) == '<')
      return line.length();

    for (int c = in.read(); c != -1; c = in.read()) {
      switch (c) {
        case '\r':
          if (_peek(in) == '\n') {
            in.read();
          }
        case '\n': 
          if (line.length() > 0) {
            // at EOL -- check for continued line if the current
            // (possibly continued) line wasn't blank
            if (allowContinuedLine) 
              switch (_peek(in)) {
                case ' ' : case '\t':                   // line is continued
                  in.read();
                  continue;
              }
          }
          return line.length();      // else complete
        default :
          line.append((char)c);
      }
    }
    // we should never get here
    line.setLength(0);
    return 0;
  }

  // shamelessly forklifted from nutch
  private static int _peek(PushbackInputStream in)
      throws IOException {
    int value = in.read();
    in.unread(value);
    return value;
  }

}

