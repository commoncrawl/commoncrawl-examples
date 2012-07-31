package org.commoncrawl.hadoop.mapred;

import org.junit.Assert;
import org.junit.Test;

public class Test {

  ArcRecordReaderCC _reader;

  @Before
  public void setUp() {
    this._reader = new ArcRecordReaderCC();
  }
	
  @Test
  public void test__readLine() {
    
    String s = "Hello!\r\nThis is a new line!\n\nHello";
    ArcRecordReaderCC r = new ArcRecordReaderCC(new ByteArrayInputStream(s.getBytes("UTF-8")));

    int n;

    n = r.readLine();
    Assert.assertTrue(r._buffer.toString().equals("Hello!"));
    
  }

  @Test
  public void test__readLine() {
    
    String a = "Hello!
  }
}
