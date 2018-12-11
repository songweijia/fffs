package edu.cornell.cs.blog;

import java.nio.ByteBuffer;

public class ts64RecordParser implements IRecordParser {
  long ts;
  @Override
  public long getUserTimestamp() {
    return ts;
  }

  @Override
  public int ParseRecord(ByteBuffer bb) throws RecordParserException {
    if (bb.remaining() != 8) {
      throw new RecordParserException("ts64 record should be exactly 8 bytes instead of " + bb.remaining() + " bytes.");
    } else {
      ts = 0l;
      for (int i = 0; i < 8; i++) {
        ts += (long) bb.get();
        if (i + 1 < 8)
          ts = ts << 8;
      }
      return bb.position();
    }
  }
  
  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    return ParseRecord(ByteBuffer.wrap(buf,offset,len));
  }
}
