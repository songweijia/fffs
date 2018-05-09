/**
 * 
 */
package edu.cornell.cs.blog;

import java.nio.ByteBuffer;

/**
 * @author weijia
 * A RecordParser finds the boundary of each record in a stream and
 * retrieves the user-defined timestamp.
 */
public interface IRecordParser {
  /**
   * @param record
   * @return user defined timestamp parsed. -1L for invalid timestamp.
   */
  public long getUserTimestamp();
  
  /**
   * ParseRecord for a 
   * @param buf
   * @param offset beginning of the buffer
   * @param len length of the buffer
   * @return beginning of the next record. -1L for incomplete record.
   * @throws RecordParserException: parse failed. 
   */
  public int ParseRecord(byte[] buf, int offset, int len) throws RecordParserException;

  /**
   * ParseRecord for a 
   * @param bb
   * @return beginning of the next record. -1L for incomplete record.
   * @throws RecordParserException: parse failed. 
   */
  public int ParseRecord(ByteBuffer bb)throws RecordParserException;
  
  @SuppressWarnings("serial")
  public class RecordParserException extends Exception{
    public RecordParserException(){
      super();
    }
    public RecordParserException(String msg){
      super(msg);
    }
  }
}
