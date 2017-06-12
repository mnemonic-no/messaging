package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.utilities.collections.ListUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JMSUtilsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testMessageDigestWithNullStringNotAllowed() {
    assertEquals(null, JMSUtils.md5(null));
  }

  @Test
  public void testMessageDigestString() {
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", JMSUtils.md5(new byte[]{}));
    assertEquals("5289df737df57326fcdd22597afb1fac", JMSUtils.md5(new byte[]{1,2,3}));
  }

  @Test
  public void testMessageDigester() {
    MessageDigest digester = JMSUtils.md5();
    assertEquals("5289df737df57326fcdd22597afb1fac", JMSUtils.hex(digester.digest(new byte[]{1,2,3})));
  }

  @Test
  public void testHex() {
    assertEquals(null, JMSUtils.hex(null));
    assertEquals("", JMSUtils.hex(new byte[]{}));
    assertEquals("01", JMSUtils.hex(new byte[]{1}));
    assertEquals("0a", JMSUtils.hex(new byte[]{10}));
    assertEquals("7f", JMSUtils.hex(new byte[]{0x7f}));
    assertEquals("ff", JMSUtils.hex(new byte[]{-1}));
    assertEquals("01020304", JMSUtils.hex(new byte[]{1, 2, 3, 4}));
    assertEquals("0f101112", JMSUtils.hex(new byte[]{15, 16, 17, 18}));
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    Serializable obj = "abc";
    byte[] bytes = JMSUtils.serialize(obj);
    assertEquals(obj, JMSUtils.unserialize(bytes));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSplitEmptyArrayNotAllowed() {
    JMSUtils.splitArray(new byte[]{}, 4);
  }

  @Test
  public void testSplitArray() {
    assertTrue(equals(
            ListUtils.list(new byte[]{1,2}),
            JMSUtils.splitArray(new byte[]{1,2}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1,2,3,4}),
            JMSUtils.splitArray(new byte[]{1,2,3,4}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1,2,3,4}, new byte[]{5,6}),
            JMSUtils.splitArray(new byte[]{1,2,3,4,5,6}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1,2,3,4}, new byte[]{5,6,7,8}),
            JMSUtils.splitArray(new byte[]{1,2,3,4,5,6,7,8}, 4)
    ));
  }

  //helpers

  private boolean equals(List<byte[]> a, List<byte[]> b) {
    if (a == null || b == null) return false;
    if (a.size() != b.size()) return false;
    for (int i = 0; i < a.size(); i++) {
      if (!equals(a.get(i), b.get(i))) return false;
    }
    return true;
  }

  private boolean equals(byte[] a, byte[] b) {
    if (a == null || b == null) return false;
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

}
