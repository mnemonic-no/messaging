package no.mnemonic.messaging.requestsink.jms;

import no.mnemonic.commons.utilities.collections.ListUtils;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import static no.mnemonic.messaging.requestsink.jms.JMSRequestProxy.PROPERTY_FRAGMENTS_IDX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class JMSUtilsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testMessageDigestWithNullStringNotAllowed() {
    assertEquals(null, JMSUtils.md5(null));
  }

  @Test
  public void testMessageDigestString() {
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", JMSUtils.md5(new byte[]{}));
    assertEquals("5289df737df57326fcdd22597afb1fac", JMSUtils.md5(new byte[]{1, 2, 3}));
  }

  @Test
  public void testMessageDigester() {
    MessageDigest digester = JMSUtils.md5();
    assertEquals("5289df737df57326fcdd22597afb1fac", JMSUtils.hex(digester.digest(new byte[]{1, 2, 3})));
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
  public void testFragmenter() throws JMSException, IOException {
    JMSUtils.FragmentConsumer consumer = mock(JMSUtils.FragmentConsumer.class);
    byte[] data = barray(1, 2, 3, 4, 5, 6, 7);
    byte[] digest = JMSUtils.md5().digest(data);
    JMSUtils.fragment(new ByteArrayInputStream(data), 3, consumer);
    verify(consumer).fragment(argThat(b -> Arrays.equals(b, barray(1, 2, 3))), eq(0));
    verify(consumer).fragment(argThat(b -> Arrays.equals(b, barray(4, 5, 6))), eq(1));
    verify(consumer).fragment(argThat(b -> Arrays.equals(b, barray(7))), eq(2));
    verify(consumer).end(eq(3), argThat(b -> Arrays.equals(b, digest)));
  }

  @Test
  public void testReassembleFragments() throws IOException, JMSException {
    byte[] data = barray(1, 2, 3, 4, 5, 6, 7);
    byte[] digest = JMSUtils.md5().digest(data);
    byte[] result = JMSUtils.reassembleFragments(
            ListUtils.list(
                    prepareFragment(0, new byte[]{1, 2, 3}),
                    prepareFragment(1, new byte[]{4, 5, 6}),
                    prepareFragment(2, new byte[]{7})
            ),
            3, JMSUtils.hex(digest));
    assertTrue(Arrays.equals(data, result));
  }

  @Test
  public void testReassembleFragmentsOutOfOrder() throws IOException, JMSException {
    byte[] data = barray(1, 2, 3, 4, 5, 6, 7);
    byte[] result = JMSUtils.reassembleFragments(
            ListUtils.list(
                    prepareFragment(1, new byte[]{4, 5, 6}),
                    prepareFragment(0, new byte[]{1, 2, 3}),
                    prepareFragment(2, new byte[]{7})
            ),
            3, JMSUtils.hex(JMSUtils.md5().digest(data)));
    assertTrue(Arrays.equals(data, result));
  }

  @Test(expected = JMSException.class)
  public void testReassembleFragmentsFailsOnInvalidDigest() throws IOException, JMSException {
    JMSUtils.reassembleFragments(
            ListUtils.list(
                    prepareFragment(0, new byte[]{1, 2, 3}),
                    prepareFragment(1, new byte[]{4, 5, 6}),
                    prepareFragment(2, new byte[]{7})
            ),
            3, JMSUtils.hex(new byte[]{1, 1, 2, 2, 3, 3, 4, 4}));
  }

  @Test(expected = JMSException.class)
  public void testReassembleFragmentsFailsOnMissingFragment() throws IOException, JMSException {
    byte[] data = barray(1, 2, 3, 4, 5, 6, 7);
    JMSUtils.reassembleFragments(
            ListUtils.list(
                    prepareFragment(0, new byte[]{1, 2, 3}),
                    prepareFragment(2, new byte[]{7})
            ),
            3, JMSUtils.hex(JMSUtils.md5().digest(data)));
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
            ListUtils.list(new byte[]{1, 2}),
            JMSUtils.splitArray(new byte[]{1, 2}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1, 2, 3, 4}),
            JMSUtils.splitArray(new byte[]{1, 2, 3, 4}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1, 2, 3, 4}, new byte[]{5, 6}),
            JMSUtils.splitArray(new byte[]{1, 2, 3, 4, 5, 6}, 4)
    ));
    assertTrue(equals(
            ListUtils.list(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8}),
            JMSUtils.splitArray(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, 4)
    ));
  }

  //helpers

  private MessageFragment prepareFragment(int idx, byte[] data) throws JMSException {
    BytesMessage bytesMessage = Mockito.mock(BytesMessage.class);
    when(bytesMessage.getIntProperty(PROPERTY_FRAGMENTS_IDX)).thenReturn(idx);
    when(bytesMessage.getBodyLength()).thenReturn((long) data.length);
    when(bytesMessage.readBytes(any())).thenAnswer(i -> {
      System.arraycopy(data, 0, i.getArgument(0), 0, data.length);
      return null;
    });
    return new MessageFragment(bytesMessage);
  }

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

  public static byte[] barray(int... values) {
    byte[] result = new byte[values.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = (byte) values[i];
    }
    return result;
  }

}
