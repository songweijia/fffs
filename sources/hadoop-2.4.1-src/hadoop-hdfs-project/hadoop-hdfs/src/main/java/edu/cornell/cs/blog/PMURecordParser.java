package edu.cornell.cs.blog;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class PMURecordParser implements IRecordParser {
  private long ts;

  private static int intFromArray(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer.getInt();
  }

  private static long longFromArray(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer.getLong();
  }

  private static byte[] fillZeros(byte[] payload, int offset, int length) {
    for (int i = 0; i < length; i++)
      payload[offset+i] = 0;
    return payload;
  }

  @Override
  public long getUserTimestamp() {
    return ts;
  }

  @Override
  public int ParseRecord(ByteBuffer bb) throws RecordParserException {
    byte[] data = new byte[14];
    int rem_size = bb.remaining();
    int rec_size;
    long s, us;
    byte[] buf;

    // Sanity checks.
    if (rem_size == 0)
      return -1;
    assert rem_size >= 16;

    // Get header.
    bb.get(data, 0, 14);
    buf = fillZeros(Arrays.copyOfRange(data,0,4),0,2);
    rec_size = intFromArray(buf);
    buf = fillZeros(Arrays.copyOfRange(data,2,10),0,4);
    s = longFromArray(buf);
    buf = fillZeros(Arrays.copyOfRange(data,6,14),0,5);
    us = longFromArray(buf);

    // Assume that packet holds one or more full records. Records are not partially written.
    assert rem_size >= rec_size;

    // Calculate timestamp and return size.
    ts = 1000000*s + us;
    return rec_size;
  }

  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    return ParseRecord(ByteBuffer.wrap(buf,offset,len));
  }
}
