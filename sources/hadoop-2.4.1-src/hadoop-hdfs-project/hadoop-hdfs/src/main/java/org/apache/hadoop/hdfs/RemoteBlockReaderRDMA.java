/**
 * 
 */
package org.apache.hadoop.hdfs;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.client.ClientMmap;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReadOpChecksumInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import edu.cornell.cs.blog.JNIBlog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * @author weijia
 * 
 */
@InterfaceAudience.Private
public class RemoteBlockReaderRDMA implements BlockReader {
  static final Log LOG = LogFactory.getLog(RemoteBlockReaderRDMA.class);
  static long hRDMABufferPool = JNIBlog.getRDMABufferPool();
  static final int RDMA_CON_PORT;

  static{
    Configuration conf = new HdfsConfiguration();
    RDMA_CON_PORT = conf.getInt(DFSConfigKeys.DFS_RDMA_CON_PORT_KEY, DFSConfigKeys.DFS_RDMA_CON_PORT_DEFAULT);
  }
  
  final private Peer peer;
  final private DatanodeID datanodeID;
  final private PeerCache peerCache;
  final private boolean isLocal;
  private JNIBlog.RBPBuffer rbpBuffer;
  private long startOffset;
  private long bytesToRead;
  final private String filename;
  private long timestamp;
  private boolean bUserTimestamp;
  
  /**
   * @param file
   * @param bpid
   * @param blockId
   * @param startOffset
   * @param bytesToRead
   * @param peer
   * @param datanodeID
   * @param peerCache
   */
  protected RemoteBlockReaderRDMA(String file, String bpid, long blockId,
      long startOffset, long bytesToRead, 
      Peer peer, DatanodeID datanodeID, PeerCache peerCache, long timestamp,
      boolean bUserTimestamp){
    this.isLocal = DFSClient.isLocalAddress(NetUtils.
        createSocketAddr(datanodeID.getXferAddr()));
    this.peer = peer;
    this.peerCache = peerCache;
    this.datanodeID = datanodeID;
    this.timestamp = timestamp;
    this.bUserTimestamp = bUserTimestamp;
    try{
      //connect to server.
      JNIBlog.rbpConnect(hRDMABufferPool, peer.getRemoteIPString().getBytes());
      // allocate buffer.
      rbpBuffer = JNIBlog.rbpAllocateBlockBuffer(hRDMABufferPool);
    }catch(Exception e){
      LOG.error("Fail to allocate rdma client side block buffer.");
      System.exit(-1);
    }
    startOffset = Math.max(startOffset, 0);
    this.bytesToRead = bytesToRead;
    this.filename = file;
    // set the buffer viewport.
    rbpBuffer.buffer.position((int)startOffset);
    rbpBuffer.buffer.mark();
    rbpBuffer.buffer.limit((int)(startOffset+bytesToRead));
  }
  
  /**
   * @param file
   * @param block
   * @param blockToken
   * @param startOffset
   * @param len
   * @param clientName
   * @param peer
   * @param datanodeID
   * @param peerCache
   * @return
   * @throws IOException
   */
  public static BlockReader newBlockReader(String file,
      ExtendedBlock block,
      Token<BlockTokenIdentifier> blockToken,
      long startOffset,
      long len,
      String clientName,
      Peer peer, DatanodeID datanodeID,
      PeerCache peerCache, long timestamp,
      boolean bUserTimestamp) throws IOException{
    RemoteBlockReaderRDMA blockReader = new RemoteBlockReaderRDMA(file,
        block.getBlockPoolId(), block.getBlockId(), startOffset, len, peer,
        datanodeID, peerCache, timestamp, bUserTimestamp);
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
        peer.getOutputStream()));
    // send request
    new Sender(out).readBlockRDMA(block, blockToken, clientName, 
        JNIBlog.getPid(), startOffset, len, blockReader.rbpBuffer.address,
        timestamp,bUserTimestamp);
    // get response
    DataInputStream in = new DataInputStream(peer.getInputStream());
    BlockOpResponseProto status = BlockOpResponseProto.parseFrom(
        PBHelper.vintPrefixed(in));
    checkSuccess(status, peer, block, file);
    return blockReader;
  }  
  
  /**
   * @param status
   * @param peer
   * @param block
   * @param file
   * @throws IOException
   */
  static void checkSuccess(
      BlockOpResponseProto status, Peer peer,
      ExtendedBlock block, String file)
      throws IOException {
    if (status.getStatus() != Status.SUCCESS) {
      if (status.getStatus() == Status.ERROR_ACCESS_TOKEN) {
        throw new InvalidBlockTokenException(
            "Got access token error for OP_READ_BLOCK_RDMA, self="
                + peer.getLocalAddressString() + ", remote="
                + peer.getRemoteAddressString() + ", for file " + file
                + ", for pool " + block.getBlockPoolId() + " block " 
                + block.getBlockId() + "_" + block.getGenerationStamp());
      } else {
        throw new IOException("Got error for OP_READ_BLOCK_RDMA, self="
            + peer.getLocalAddressString() + ", remote="
            + peer.getRemoteAddressString() + ", for file " + file
            + ", for pool " + block.getBlockPoolId() + " block " 
            + block.getBlockId() + "_" + block.getGenerationStamp());
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.ByteBufferReadable#read(java.nio.ByteBuffer)
   */
  @Override
  public int read(ByteBuffer buf) throws IOException {
    if(rbpBuffer.buffer.remaining() == 0)
      return -1;
    
    int nRead = Math.min(rbpBuffer.buffer.remaining(), buf.remaining());
    ByteBuffer writeSlice = rbpBuffer.buffer.duplicate();
    writeSlice.limit(writeSlice.position() + nRead);
    rbpBuffer.buffer.put(writeSlice);
    rbpBuffer.buffer.position(writeSlice.position());
    return nRead;
  }

  // this is a new API
  public ByteBuffer readAll() throws IOException{
    ByteBuffer bb = rbpBuffer.buffer.asReadOnlyBuffer();
    rbpBuffer.buffer.position(rbpBuffer.buffer.limit());
    return bb;
  }
  public ByteBuffer read(int len) throws IOException{
    ByteBuffer bb = rbpBuffer.buffer.asReadOnlyBuffer();
    if(bb.limit() - bb.position() > len)
      bb.limit(bb.position()+len);
    return bb;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#read(byte[], int, int)
   */
  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if(rbpBuffer.buffer.remaining() == 0)
      return -1;
    
    int nRead = Math.min(rbpBuffer.buffer.remaining(), len);
    rbpBuffer.buffer.get(buf,off,nRead);
    
    return nRead;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    int toSkip = (int)Math.min(n, rbpBuffer.buffer.remaining());
    rbpBuffer.buffer.position(rbpBuffer.buffer.position() + toSkip);
    return toSkip;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#available()
   */
  @Override
  public int available() throws IOException {
    return rbpBuffer.buffer.remaining();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#close()
   */
  @Override
  public void close() throws IOException {
    try{
      JNIBlog.rbpReleaseBuffer(hRDMABufferPool, rbpBuffer);
      rbpBuffer = null;
    }catch(Exception e){
      LOG.warn("cannot release RDMA buffer with exception:"+e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#readFully(byte[], int, int)
   */
  @Override
  public void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException {
    if(amtToRead > rbpBuffer.buffer.remaining())
      throw new IOException("try to read " + amtToRead + " bytes but we have only " + 
        rbpBuffer.buffer.remaining() + " bytes remains.");
    read(buf,readOffset,amtToRead);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#readAll(byte[], int, int)
   */
  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return read(buf,offset,len);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#isLocal()
   */
  @Override
  public boolean isLocal() {
    return isLocal;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#isShortCircuit()
   */
  @Override
  public boolean isShortCircuit() {
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hdfs.BlockReader#getClientMmap(java.util.EnumSet)
   */
  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    return null;
  }

}
