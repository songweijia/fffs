/**
 * 
 */
package edu.cornell.cs.blog;
import java.nio.ByteBuffer;

/**
 * @author weijia
 * The default record parser just return what we have 
 * in the buffer with an invalid timestamp.
 */
public class DefaultRecordParser implements IRecordParser {

  /* (non-Javadoc)
   * @see edu.cornell.cs.blog.IRecordParser#getUserTimestamp(byte[])
   */
  @Override
  public long getUserTimestamp() {
    return -1L;
  }

  @Override
  public int ParseRecord(ByteBuffer bb) throws RecordParserException {
    if (bb == null || bb.remaining() == 0) {
      throw new RecordParserException("Cannot ParseRecord");
    } else {
      return bb.remaining();
    }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.blog.IRecordParser#ParseRecord(byte[], int, int)
   */
  @Override
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException {
    if (offset >= 0 && offset + len < buf.length)
      return ParseRecord(ByteBuffer.wrap(buf,offset,len));
    else
      throw new RecordParserException("Cannot ParseRecord");
  }
}
