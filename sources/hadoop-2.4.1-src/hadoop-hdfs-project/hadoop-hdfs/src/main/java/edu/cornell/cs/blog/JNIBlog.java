package edu.cornell.cs.blog;

import edu.cornell.cs.sa.HybridLogicalClock;
import java.nio.charset.*;
import java.util.Arrays;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.RemoteBlockReaderRDMA;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;

public class JNIBlog
{
  static {
    System.loadLibrary("edu_cornell_cs_blog_JNIBlog");
  }
  static final Log LOG = LogFactory.getLog(JNIBlog.class);
  static long hRDMABufferPool = 0l;
  static public long getRDMABufferPool(){
    if(hRDMABufferPool == 0l)
      initializeRDMABufferPool();
    return hRDMABufferPool;
  }
  static private void initializeRDMABufferPool(){
    Configuration conf = new HdfsConfiguration();
    int psz = conf.getInt(DFSConfigKeys.DFS_RDMA_CLIENT_MEM_REGION_SIZE_EXPONENT_KEY, 
        DFSConfigKeys.DFS_RDMA_CLIENT_MEM_REGION_SIZE_EXPONENT_DEFAULT);
    long bs = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    int align = 0;
    int port = conf.getInt(DFSConfigKeys.DFS_RDMA_CON_PORT_KEY, DFSConfigKeys.DFS_RDMA_CON_PORT_DEFAULT);
    String dev = conf.getTrimmed(DFSConfigKeys.DFS_RDMA_DEVICE_KEY, DFSConfigKeys.DFS_RDMA_DEVICE_DEFAULT);
    while(((bs>>align)&1) == 0)align++;
    try{
      hRDMABufferPool = JNIBlog.rbpInitialize(psz, align, dev, port);
      if(hRDMABufferPool != 0L){
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
          public void run(){
            try{
              JNIBlog.rbpDestroy(hRDMABufferPool);
            }catch(IOException e){
              LOG.error("JNIBlog.rbpDestroy("+hRDMABufferPool+") failed with exception:"+e);
            }
          }
        });
      }
    }catch(Exception e){
      LOG.fatal("Fail to initialize the rdma buffer with exception:"+e);
      System.exit(-1);
    }
  }

  static public long CURRENT_SNAPSHOT_ID = -1l;
  private HybridLogicalClock hlc;
  private long jniData;
  private String persPath;
  private String bpid; // initialize it!!!
  private MemDatasetManager dsmgr;

  /**
   * Initialize both the block log and the block map.
   * @param dsmgr
   * @param bpid
   * @param poolSize
   * @param blockSize
   * @param pageSize
   * @param persPath
   * @param useRDMA - use RDMA or not?
   * @param dev - the device name for the RDMA device. 'mlx5_0' for example. if null, it chooses the first device found.
   * @param port - the port number for RDMA based Blog
   */
  public void initialize(MemDatasetManager dsmgr, String bpid, long poolSize, int blockSize, int pageSize, String persPath, boolean useRDMA, String dev, int port){
    this.dsmgr = dsmgr;
    this.bpid = bpid;
    this.persPath = persPath;
    this.blockMaps = new HashMap<Long, MemBlockMeta>();

    // initialize blog
    writeLine("Initialize called.");
    if(useRDMA) {
      writeLine("Initialize for RDMA.");
      initializeRDMA(poolSize, (int)blockSize, pageSize, persPath, dev, port);
    } else {
      writeLine("Initialize without RDMA.");
      initialize(poolSize, (int)blockSize, pageSize, persPath);
    }

    // add shutdown hook, this will destroy the blog.
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run(){
        JNIBlog.this.destroy();
      }
    });
  }
  
/** we do not need to flush block map meta data anymore. 
 * we just reconstruct them from the blog.
  // write the block map to disk
  // format:
  // int: number of blocks
  // for each block:
  //   [long:block id][long:timestamp][boolean:isdeleted][int:state]
  private synchronized void flushBlockMap(){
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try{
      fos = new FileOutputStream(this.persPath+System.getProperty("file.separator")+BLOCKMAP_FILE);
      dos = new DataOutputStream(fos);
      dos.writeInt(this.blockMaps.size()); // write number of blocks
      for(Entry<Long, MemBlockMeta> entry: this.blockMaps.entrySet()){
        MemBlockMeta mbm = entry.getValue();
        dos.writeLong(entry.getKey()); // write block id
        dos.writeLong(mbm.getGenerationStamp()); // write timestamp
        dos.writeBoolean(mbm.isDeleted()); // write delete
        dos.writeInt(mbm.getState().getValue()); // write state
      }
    }catch(IOException ioe){
      //LOG error
      System.err.println("ERROR:Cannot open blockmap file for write:"+ioe);
    }finally{
      try{
        if(dos!=null)dos.close();
        if(fos!=null)fos.close();
      }catch(IOException e){
        //do nothing
      }
    }
  }
  
  // load the block map from disk
  private synchronized void loadBlockMap(){
    FileInputStream fis = null;
    DataInputStream dis = null;
    try{
      this.blockMaps = new HashMap<Long, MemBlockMeta>();
      fis = new FileInputStream(this.persPath+System.getProperty("file.separator")+BLOCKMAP_FILE);
      dis = new DataInputStream(fis);
      int nblock = dis.readInt();
      while(nblock-- > 0){
        long bid = dis.readLong();
        long gen = dis.readLong();
        boolean del = dis.readBoolean();
        ReplicaState rs = ReplicaState.getState(dis.readInt());
        MemBlockMeta mbm = dsmgr.new MemBlockMeta(this, gen, bid, rs);
        if(del)mbm.delete();
        this.blockMaps.put(bid, mbm);
      }
    }catch(IOException ioe){
      System.err.println("WARNING:Cannot open blockmap file for read: "+ioe+". This may be caused by a new startup");
    }finally{
      try{
        if(dis!=null)dis.close();
        if(fis!=null)fis.close();
      }catch(IOException e){
        //do nothing
      }
    }
  }
*/  
  /**
   * initialization with RDMA
   * @param blockSize - block size for each block
   * @param poolSize - memory pool size
   * @param pageSize - page size
   * @param persPath - initialize it
   * @param dev - the device name for the RDMA device. 'mlx5_0' for example. if null, it chooses the first device found.
   * @param port - server port for the RDMA datanode.
   * @return error code, 0 for success.
   */
  private native int initializeRDMA(long poolSize, int blockSize, int pageSize, String persPath, String dev, int port);

  /**
   * initialization without RDMA
   * @param poolSize - memory pool size
   * @param blockSize - block size for each block
   * @param pageSize - page size
   * @param persPath - initialize it
   * @return error code, 0 for success.
   */
  private native int initialize(long poolSize, int blockSize, int pageSize, String persPath);
  
  /**
   * Replay block log on startup. This will materialize blockMaps. This
   * will be called from native initialize(). 
   * op:
   *   BOL = 0
   *   CREATE_BLOCK = 1
   *   DELETE_BLOCK = 2
   *   WRITE_BLOCK = 3
   *   SET_GENSTAMP = 4
   * genStamp: for SET_GENSTAMP only.
   */
  private void replayLogOnMetadata(long blockId, int op, long genStamp){
    if(op == 0) // do nothing for BOL
      return;
    MemBlockMeta mbm = this.blockMaps.get(blockId);
    if(mbm == null && op == 1){ // CREATE_BLOCK
      mbm = dsmgr.new MemBlockMeta(this,genStamp,blockId,ReplicaState.FINALIZED);
    }
    else if(mbm == null){
      //create block on an existing block.
    }else if(op == 2) // DELETE_BLOCK
      mbm.delete();
    else if(op==3){//WRITE_BLOCK
      // do nothing
    }
    else if(op == 4)
      mbm.setGenerationStamp(genStamp);
    this.blockMaps.put(blockId, mbm);
  }
  
  /**
   * destroy the blog
   */
  public native void destroy();
  
  /**
   * set the generationStamp of a blog.
   * @param mhlc message hybridLogicalClock
   * @param blockId
   * @param genStamp
   * @return
   */
  public native int setGenStamp(HybridLogicalClock mhlc, long blockId, long genStamp);
  
  /**
   * create a block
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int createBlock(HybridLogicalClock mhlc, long blockId, long genStamp);
  
  /**
   * @param mvc - vector clock of the driven message
   * @param blockId - block identifier
   * @return error code, 0 for success.
   */
  public native int deleteBlock(HybridLogicalClock mhlc, long blockId);
  
  /**
   * Read the latest status, using TCP/IP
   * @return error code, number of bytes read for success.
   */
  public native int readBlock(long blockId, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * Read the latest status, using RDMA
   * @param clientIp
   * @param rpid - the pid of the client
   * @param vaddr
   * @return error code, 0 for success, otherwise error code.
   */
  public native int readBlockRDMA(long blockId, int blkOfst, int length,
    byte clientIp[], int rpid, long vaddr);
  
  /**
   * Read block status specified by time t, using TCP/IP
   * get number of bytes we have in the block
   * @param t - time to read from
   * @param byUserTimestamp
   * @return error code, number of bytes read for success.
   */
  public native int readBlock(long blockId, long t, int blkOfst, int bufOfst, int length, byte buf[], 
    boolean byUserTimestamp);
  
  /**
   * Read block status specified by time t, using RDMA
   * @param t - time to read from
   * @param byUserTimestamp
   * @param clientIp
   * @param rpid - the pid of the client
   * @param vaddr
   * @return error code, 0 for success, otherwise error code.
   */
  public native int readBlockRDMA(long blockId, long t, int blkOfst, int length,
     byte clientIp[], int rpid, long vaddr, boolean byUserTimestamp);

  /**
   * for compatibility:
   * @param blockId
   * @param t
   * @param blkOfst
   * @param bufOfst
   * @param length
   * @param buf
   * @return
   */
  public int readBlock(long blockId, long t, int blkOfst, int bufOfst, int length, byte buf[]){
    return readBlock(blockId,t,blkOfst,bufOfst,length,buf,false);
  }
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @return
   */
  public native int getNumberOfBytes(long blockId);
  
  /**
   * get number of bytes we have in the block
   * @param blockId
   * @param t
   * @param byUserTimestamp
   * @return
   */
  public native int getNumberOfBytes(long blockId, long t, boolean byUserTimestamp);
  
  /**
   * @param mhlc
   * @param rp record parser
   * @param blockId
   * @param blkOfst
   * @param length
   * @param clientIp - ip address of the remote.
   * @param rpid - the pid of the client
   * @param vaddr - virtual address.
   * @return
   */
  public native int writeBlockRDMA(HybridLogicalClock mhlc, IRecordParser rp, long blockId, int blkOfst, int length,
      byte clientIp[], int rpid, long vaddr);
  
  /**
   * write to block, using TCP/IP
   * @param mhlc - message block
   * @param userTimestamp - the user timestamp
   * @return
   */
  public native int writeBlock(HybridLogicalClock mhlc, long userTimestamp, long blockId, int blkOfst, int bufOfst, int length, byte buf[]);
  
  /**
   * readLocalRTC clock. all timestamp in the log should read
   * from here.
   * @return microseconds from the Epoch(1970-01-01 00:00:00 +0000(UTC))
   */
  public static native long readLocalRTC();
  
  /**
   * @return pid of my process.
   */
  public static native int getPid();
  
  
  //////////////////////////////////////////////////////////////////////
  // Move Pool Data to JNIBlog.
  public HashMap<Long,MemBlockMeta> blockMaps;
  //////////////////////////////////////////////////////////////////////
  
  //////////////////////////////////////////////////////////////////////
  // The interface for RDMA access library
  public static class RBPBuffer{
    public long address; // address of this buffer.
    public ByteBuffer buffer; // the buffer  
  }
  /**
   * function: initialize an RDMA Buffer Pool.
   * @param psz - size of the buffer pool = 1l<<psz.
   * @param align - alignment for the buffer/page allocation = 1l<<align
   * @param dev - the device name for the RDMA device. 'mlx5_0' for example. if null, it chooses the first device found.
   * @param port - port for the blog server to listen for. ZERO FOR CLIENT.
   * @return handle of the RDMA Block Pool
   * @throws Exception
   */
  static public native long rbpInitialize(int psz, int align, String dev, int port) throws IOException;
  /**
   * function: destroy the RDMA buffer Pool.
   * @param size - size of the buffer tool
   * @throws Exception
   */
  static public native void rbpDestroy(long hRDMABufferPool) throws IOException;
  /**
   * function: allocate a block buffer.
   * @return allocated Block Buffer
   * @throws Exception
   */
  static public native RBPBuffer rbpAllocateBlockBuffer(long hRDMABufferPool) throws IOException;
  /**
   * function: release a buffer.
   * @param buf
   * @throws Exception
   */
  static public native void rbpReleaseBuffer(long hRDMABufferPool, RBPBuffer buf) throws IOException;
  /**
   * function: connect to RDMA datanode
   * @rbpBuffer handle of the RDMA buffer pool
   * @param hostIp
   * @throws Exception
   */
  static public native void rbpConnect(long hRDMABufferPool, byte hostIp[]) throws IOException;
  
  /**
   * function: do RDMA Write, this is called by the DataNode.
   * @param clientIp like "192.168.100.1".toByteArra()
   * @param address
   * @param length
   * @param pageList
   * @throws Exception
   */
  public native void rbpRDMAWrite(byte clientIp[], long address, long []pageList) throws IOException;
  
  //////////////////////////////////////////////////////////////////////
  // Tests.
  public void testBlockCreation(HybridLogicalClock mhlc)
  {
    for (long i = 0; i < 100; i++) {
      assert (createBlock(mhlc,i,0l) == 0);
      writeLine("Block " + i + " was created.");
    }
    for (long i = 4096; i < 4196; i++) {
      assert (createBlock(mhlc,i,0l) == 0);
      writeLine("Block " + i + " was created.");
    }
    assert (createBlock(mhlc,1,0l) == -1);
    assert (createBlock(mhlc,4096,0l) == -1);
    assert (createBlock(mhlc,4100,0l) == -1);
    assert (createBlock(mhlc,5000,0l) == 0);
    writeLine("Block 5000 was created.");
  }
  
  public void testBlockDeletion(HybridLogicalClock mhlc)
  { 
    for (long i = 0; i < 100; i++) {
      assert (deleteBlock(mhlc,i) == 0);
      writeLine("Block " + i + " was deleted.");
    }
    assert (deleteBlock(mhlc,1) == -1);
    assert (deleteBlock(mhlc,4500) == -1);
    assert (deleteBlock(mhlc,4100) == 0);
    writeLine("Block 4100 was deleted.");
  }
  
  public long testWrite(HybridLogicalClock mhlc) throws InterruptedException
  {
    String a = "Hello Theo & Weijia. I am working well!!";
    String b = "This should not be in the snapshot read.";
    String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
    byte[] d = new byte[4096*3];
    byte[] e = new byte[4096*3];
    long rtc;

    for (int i = 0; i < 4096; i++) {
    	d[3*i] = (byte) 'a';
    	d[3*i+1] = (byte) 'b';
    	d[3*i+2] = (byte) 'c';
    	e[3*i] = (byte) 'x';
    	e[3*i+1] = (byte) 'y';
    	e[3*i+2] = (byte) 'z';
    }
    assert (writeBlock(mhlc, mhlc.r, 4101, 0, 0, a.length(), a.getBytes()) == 0);
    writeLine("Block 4101 was written.");
    assert (writeBlock(mhlc, mhlc.r, 4100, 0, 0, 20, a.getBytes()) == -1);
    assert (writeBlock(mhlc, mhlc.r, 4101, 100, 0, 20, a.getBytes()) == -2);
    Thread.sleep(1);
    rtc = readLocalRTC();
    assert (writeBlock(mhlc, mhlc.r, 4101, 0, 0, b.length(), b.getBytes()) == 0);
    writeLine("Block 4101 was written.");
    assert (writeBlock(mhlc, mhlc.r, 5000, 0, 0, c.length(), c.getBytes()) == 0);
    writeLine("Block 5000 was written.");
    assert (writeBlock(mhlc, mhlc.r, 5000, 40, 0, 4096*3, d) == 0);
    writeLine("Block 5000 was appended.");
    assert (writeBlock(mhlc, mhlc.r, 5000, 40+4096, 4096, 4096, e) == 0);
    writeLine("Block 5000 was written randomly.");
    
    return rtc;
  }
  
  public void testRead()
  {
	String a = "Hello Theo & Weijia. I am working well!!";
	String b = "This should not be in the snapshot read.";
	String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
	String temp;
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();
    byte[] d = new byte[4096*3];
    byte[] e = new byte[4096*3];
    
    for (int i = 0; i < 4096; i++) {
    	d[3*i] = (byte) 'a';
    	d[3*i+1] = (byte) 'b';
    	d[3*i+2] = (byte) 'c';
    	e[3*i] = (byte) 'x';
    	e[3*i+1] = (byte) 'y';
    	e[3*i+2] = (byte) 'z';
    }
    for (int i = 4096; i < 4096*2; i++)
    	d[i] = e[i];
    assert (readBlock(4100, 0, 0, 40, mybuf) == -1);
    assert (readBlock(4101, 100, 0, 40, mybuf) == -2);
    assert (readBlock(4101, 0, 0, 40, mybuf) == 40);
    temp = new String(mybuf, Charset.forName("UTF-8"));
    writeLine("Block 4101: " + temp);
    assert (b.compareTo(temp) == 0);
    assert (readBlock(5000, 40, 0, 3*4096, e) == 3*4096);
    writeLine("Block 5000.40: " + new String(e));
    assert (Arrays.equals(d, e));
    assert (getNumberOfBytes(5000) == 40+4096*3);
    assert (getNumberOfBytes(4101) == 40);
  }
  
  public void testSnapshot(long rtc)
  {
	String a = "Hello Theo & Weijia. I am working well!!";
	String b = "This should not be in the snapshot read.";
	String c = "Laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.";
	String temp;
    byte[] mybuf = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes();
    
    assert (readBlock(4101, rtc+10000, 0, 0, 40, mybuf) == -1);
    assert (readBlock(5001, rtc, 0, 0, 40, mybuf) == -2);
    assert (readBlock(5000, rtc, 0, 0, 40, mybuf) == -3);
    assert (readBlock(4101, rtc, 0, 0, 40, mybuf) == 40);
    temp = new String(mybuf, Charset.forName("UTF-8"));
    writeLine("Snapshot " + Long.toString(rtc)  + ", Block 4101: " + temp);
    assert (a.compareTo(temp) == 0);
    assert (getNumberOfBytes(4101,rtc,false) == 40);
  }
  
  static public void writeLine(String str)
  {
    System.out.println(str);
    System.out.flush();
  }
  
  /**
   * Test stub
   * @param args
   */
  public static void main(String[] args) throws InterruptedException
  {
    JNIBlog bl = new JNIBlog();
    HybridLogicalClock mhlc = new HybridLogicalClock();
    long rtc = 0;
    Configuration conf = new Configuration();
    MemDatasetManager mdm = new MemDatasetManager(conf);
    
    writeLine("Begin Initialize.");
    bl.initialize(mdm, "testpbid", 1l<<30, 1024*1024, 1024, "testbpid", false, null, 0);
    writeLine(bl.hlc.toString());
    if (!args[0].equals("pers")) {
      writeLine("Create Blocks.");
      bl.testBlockCreation(mhlc);
      writeLine("Delete Blocks.");
      bl.testBlockDeletion(mhlc);
      writeLine("Write Blocks.");
      rtc = bl.testWrite(mhlc);
      Thread.sleep(2);
    }
    writeLine("Read Blocks.");
    bl.testRead();
    if (!args[0].equals("pers")) {
      writeLine("Read Snapshot Blocks.");
      bl.testSnapshot(rtc);
    }
  }
}
