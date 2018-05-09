package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.Closeable;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;

/**
 * @author weijia
 * Send block message using RDMA.
 */
public class RDMABlockSender implements Closeable{

  /** the block to read from */
  protected final ExtendedBlock block;
  /** Position of first byte to read from block file */
  protected final long startOffset;
  /** length of data to read */
  protected final long length;
  /** DateNode */
  protected DataNode datanode;
  /** peer */
  protected final String clientIp;
  /** peer's pid */
  protected final int rpid;
  /** remote vaddress */
  protected final long vaddr;
  /** replica */
  protected MemBlockMeta replica;
  /** timestamp */
  protected long timestamp;
  /** bUsertimestamp */
  protected boolean bUserTimestamp;
  
  /**
   * Constructor 
   * @param block Block that is being read
   * @param startOffset starting 
   * @param length
   * @param datanode
   */
  RDMABlockSender(ExtendedBlock block, 
    long startOffset, long length,
    DataNode datanode, String clientIp, int rpid, long vaddr, long timestamp, boolean bUserTimestamp){
    this.block = block;
    this.startOffset = startOffset;
    this.length = Math.min(length, this.block.getNumBytes()) - startOffset;
    this.clientIp = clientIp;
    this.rpid = rpid;
    this.vaddr = vaddr;
    this.replica = (MemBlockMeta)datanode.data.getReplica(block);
    this.timestamp = timestamp;
    this.bUserTimestamp = bUserTimestamp;
  }
  
  /**
   * doSend
   */
  void doSend()
  throws IOException{
    //send data.
    replica.readByRDMA((int)startOffset, (int)length, clientIp, rpid, vaddr,
        timestamp, bUserTimestamp);
  }

  @Override
  public void close() throws IOException{
    // do nothing...
  }
}
