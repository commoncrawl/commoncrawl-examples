package org.commoncrawl.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.lang.StringBuilder;

import junit.framework.TestCase;
import static junit.framework.Assert.*;

/**
 * Unit Tests for jUnit 3.8
 */
public class TestArcRecordCC extends TestCase {

  ArcRecordCC r;

  /*
  public static junit.framework.Test suite() {
    return new junit.framework.JUnit4TestAdapter(TestArcRecordCC.class);
  }
  */

  public InputStream getPayload1()
      throws Exception {

    StringBuilder s = new StringBuilder();

    s.setLength(0);
    s.append("<html>\n");
    s.append("  <head>\n");
    s.append("    <title>This is a web page!</title>\n");
    s.append("  </head>\n");
    s.append("  <body>\n");
    s.append("    <h1>This is some content!</h1>\n");
    s.append("  </body>\n");
    s.append("</html>");

    String content = s.toString();

    s.setLength(0);
    s.append("HTTP/1.1 200 OK\r\n");
    s.append("Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n");
    s.append("Content-Type: text/html; charset=utf-8\r\n");
    s.append("\r\n");
    s.append(content);
    s.insert(0, "http://www.example.com/path/file.php?param=123,456%20789 123.123.123.123 20120235131415 text/html "+(s.length()-3)+"\n"); 

    return new ByteArrayInputStream(s.toString().getBytes("UTF-8"));
  }    

  public void setUp() {
    r = new ArcRecordCC();
  }

  public void test_getIpAddress()
      throws Exception {
    r.readFrom(this.getPayload1());
    assertEquals(r.getIpAddress(), "123.123.123.123");
  }

  public void test_getHttpHeaders()
      throws Exception {
    r.readFrom(this.getPayload1());
    assertEquals(r.getHttpResponse().getFirstHeader("Content-Type").getValue(), "text/html; charset=utf-8");
  }

  public void test_getHttpResponse_getEntity()
      throws Exception {

    r.readFrom(this.getPayload1());
    assertNotNull(r.getHttpResponse().getEntity());

    byte[] buffer = new byte[1000];
    r.getHttpResponse().getEntity().getContent().read(buffer, 0, 1000);

    StringBuilder s = new StringBuilder();
    s.append("<html>\n");
    s.append("  <head>\n");
    s.append("    <title>This is a web page!</title>\n");
    s.append("  </head>\n");
    s.append("  <body>\n");
    s.append("    <h1>This is some content!</h1>\n");
    s.append("  </body>\n");
    s.append("</html>");

    String v1 = s.toString();
    String v2 = new String(buffer, "UTF-8");

    assertEquals(v1.trim(), v2.trim());
  }

  public void test_getParsedHTML()
      throws Exception {
    r.readFrom(this.getPayload1());
    assertNotNull(r.getParsedHTML());
  }
}
