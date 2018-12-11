package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_MEMBLOCK_PAGESIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DEFAULT_DFS_MEMBLOCK_PAGESIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_RDMA_CON_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_RDMA_CON_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_RDMA_DEVICE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_RDMA_DEVICE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_RDMA_BLOCKREADER;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_RDMA_BLOCKREADER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_RDMA_BLOCKWRITER;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_RDMA_BLOCKWRITER_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.HLCOutputStream;

import edu.cornell.cs.blog.JNIBlog;
import edu.cornell.cs.blog.IRecordParser;
import edu.cornell.cs.sa.*;

public class MemDatasetManager {
  static final Log LOG = LogFactory.getLog(MemDatasetManager.class);

  /*  
  class PoolData{
    HashMap<Long,MemBlockMeta> blockMaps; // blockId->MemBlockMeta data
    JNIBlog blog; // blog for the memory
  }
*/  

//  private HashMap<ExtendedBlockId, String> diskMaps;
//  private MemDatasetImpl dataset;
  private Map<String, JNIBlog> blogMap; // where data is stored.
  private final long capacity;
  private final long blocksize; 
  private final int pagesize;
  private final String perspath; // the path for persistent files
  private final int rdmaport; // port...
  private final String rdmadev; // the rdmadev name
  private final boolean useRDMA; // are we using RDMA ?

  
//  public class MemBlockMeta extends Block implements Replica {
  public class MemBlockMeta extends ReplicaInfo {
    boolean isDeleted;
    JNIBlog blog;
    ReplicaState state;
    long accBytes;
    
    public MemBlockMeta(String bpid, long genStamp, long blockId, ReplicaState state) {
      super(blockId,(int)JNIBlog.CURRENT_SNAPSHOT_ID,0l,genStamp);
      JNIBlog blog = null;
      synchronized(blogMap){
        blog = blogMap.get(bpid);
        if(blog==null){
      	  blog = newJNIBlog(bpid);
      	  blogMap.put(bpid, blog);
        }
      }
      this.blog = blog;
      this.blockId = blockId;
      this.state = state;
      this.isDeleted = false;
      this.accBytes = 0l;
    }
    
    public MemBlockMeta(JNIBlog blog, long genStamp, long blockId, ReplicaState state) {
      super(blockId,(int)JNIBlog.CURRENT_SNAPSHOT_ID,0l,genStamp);
      this.blog = blog;
      this.blockId = blockId;
      this.state = state;
      this.isDeleted = false;
      this.accBytes = 0l;
    }
    
    public boolean isDeleted(){
    	return isDeleted;
    }
    
    public void delete(){
    	this.isDeleted = true;
    }

    public ReplicaState getState() {
      return state;
    }
    
    public void setState(ReplicaState state){
    	this.state = state;
    }
    
    public void setGenerationStamp(HybridLogicalClock mhlc,long genStamp)
    throws IOException{
      if(this.blog.setGenStamp(mhlc, this.blockId, genStamp) != 0)
        throw new IOException("Cannot setGenerationStamp on block:"+this.blockId);
      super.setGenerationStamp(genStamp);
    }

    @Override
    public long getBytesOnDisk() {
    	return getNumBytes();
    }
    public long getBytesOnDisk(long timestamp, boolean bUserTimestamp){
    	return getNumBytes(timestamp, bUserTimestamp);
    }

    @Override
    public long getVisibleLength() {
    	return getNumBytes();
    }
    
//    public long getVisibleLength(long sid){
//    	return getNumBytes(sid);
//    }
    
    public long getVisibleLength(long timestamp, boolean bUserTimestamp){
      return getNumBytes(timestamp,bUserTimestamp);
    }

    public String getStorageUuid() {
      return "0";
    }
    
    public long getBytesAcked() {
    	return getNumBytes();
    }

    @Override
    public long getBlockId() {
        return this.blockId;
    }

  	@Override
  	public long getNumBytes() {
  		return getNumBytes(JNIBlog.CURRENT_SNAPSHOT_ID,false);
  	}
  	
  	public long getNumBytes(long timestamp, boolean bUserTimestamp){
          long nb = (timestamp == JNIBlog.CURRENT_SNAPSHOT_ID)?blog.getNumberOfBytes(blockId):blog.getNumberOfBytes(blockId,timestamp,bUserTimestamp);
          if(nb == -1L)nb = 0L; // -1 means block does not exists.
          return nb;
  	}
  	
  	public BlogOutputStream getOutputStream(){
  		return getOutputStream((int)getNumBytes());
  	}

  	public BlogOutputStream getOutputStream(int offset){
  		if(offset < 0)
  			return getOutputStream();
  		else
  		  return new BlogOutputStream(blog,blockId,offset,this);
  	}

    public BlogInputStream getInputStream(int offset) {
      return getInputStream(offset, JNIBlog.CURRENT_SNAPSHOT_ID, false);
  	}

    public BlogInputStream getInputStream(int offset, long timestamp, boolean bUserTimestamp) {
      return new BlogInputStream(blog,blockId,offset,timestamp,bUserTimestamp);
  	}

    // if timestamp == -1, we read from the current state.
    public void readByRDMA(int startOffset, int length, String clientIp, int rpid, long vaddr, long timestamp,
                           boolean bUserTimestamp) throws IOException {
      long blen = (timestamp == -1) ? getNumBytes() : getNumBytes(timestamp,bUserTimestamp);
      int rc = 0;

      if (startOffset + length > blen)
        throw new IOException("readByRDMA failed: timestamp=" + timestamp + ",start=" + startOffset + ",len=" + length +
                              ",blen=" + blen);

      // rdma read
      if (timestamp == -1) { // read from latest version
        rc = blog.readBlockRDMA(blockId, startOffset, length, clientIp.getBytes(), rpid, vaddr);
      } else { // read from the specified point of time
        rc = blog.readBlockRDMA(blockId, timestamp, startOffset, length, clientIp.getBytes(), rpid, vaddr,
                                bUserTimestamp);
      }
      if (rc!=length) {
        throw new IOException("readByRDMA failed: JNIBlog.readBlockRDMA returns: " + rc);
      }
    }

  	public void writeByRDMA(int startOffset, int length, String clientIp, 
  	    int rpid, long vaddr, HybridLogicalClock mhlc, IRecordParser rp)throws IOException{
  	  long blen = getNumBytes();
  	  if(startOffset > blen)
  	    throw new IOException("writeByRDMA failed:blen="+blen+",start="+startOffset+
  	        ",len="+length+",client="+clientIp+",vaddr="+vaddr+",mhlc="+mhlc);
  	  int rc = 0;
  	  if ((rc=blog.writeBlockRDMA(mhlc, rp, blockId, startOffset, length, clientIp.getBytes(), rpid, vaddr))!=0) {
            throw new IOException("writeByRDMA failed: JNIBlog.writeBlockRDMA returns: " + rc);
  	  }
  	  //update length
  	  if(this.getNumBytes() < this.getBytesOnDisk())
  	    this.setNumBytes(this.getBytesOnDisk());
  	  this.accBytes += length;
  	}
  }
  
  class BlogInputStream extends InputStream {
    JNIBlog blog;
    long blockId;
    int offset;
    long timestamp;
    boolean bUserTimestamp;
    
    /**
     * @param bpid
     * @param blockId
     * @param offset
     * @param snapshotId
     */
    BlogInputStream(String bpid, long blockId, int offset, long timestamp, boolean bUserTimestamp) {
      this.blog = blogMap.get(bpid);
      this.blockId = blockId;
      this.offset = offset;
      this.timestamp = timestamp;
      this.bUserTimestamp = bUserTimestamp;
    }
    
    BlogInputStream(JNIBlog blog, long blockId, int offset, long timestamp, boolean bUserTimestamp) {
      this.blog = blog;
      this.blockId = blockId;
      this.offset = offset;
      this.timestamp = timestamp;
      this.bUserTimestamp = bUserTimestamp;
    }
    
    /**
     * @param blockId
     * @param offset
     */
    BlogInputStream(String bpid, int blockId, int offset) {
      this(bpid, blockId, offset, JNIBlog.CURRENT_SNAPSHOT_ID, false);
    }
    
    public synchronized int read() throws IOException {
   		byte [] b = new byte[1];
   		read(b,0,1);
   		return b[0];
    }

    /* (non-Javadoc)
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
      int endOfBlock = this.timestamp == JNIBlog.CURRENT_SNAPSHOT_ID ?
                       blog.getNumberOfBytes(blockId) :
                       blog.getNumberOfBytes(blockId, timestamp, bUserTimestamp);
      if (offset < endOfBlock) {
        int ret = this.timestamp == JNIBlog.CURRENT_SNAPSHOT_ID ?
                  blog.readBlock(blockId, offset, off, len, bytes) :
                  blog.readBlock(blockId, timestamp, offset, off, len, bytes, bUserTimestamp);
        if (ret > 0) {
          this.offset += ret;
          return ret;
        } else {
          throw new IOException("error in JNIBlog.read(" + blockId + "," + timestamp + "," + bUserTimestamp + "," +
                                offset + "," + off + "," + len + ",b):" + ret);
        }
      } else {
        throw new IOException("no more data available");
      }
    }
  }
  
  class BlogOutputStream extends HLCOutputStream {
    JNIBlog blog;
    long blockId;
    int offset;
    MemBlockMeta meta;

    BlogOutputStream(String bpid,long blockId, int offset, MemBlockMeta meta) {
    	this.blog = blogMap.get(bpid);
    	this.blockId = blockId;
    	this.offset = offset;
        this.meta = meta;
    }

    BlogOutputStream(JNIBlog blog,long blockId, int offset, MemBlockMeta meta) {
    	this.blog = blog;
    	this.blockId = blockId;
    	this.offset = offset;
        this.meta = meta;
    }

    public synchronized void write(int b) throws IOException {
      throw new IOException("Blog allows write with HLC clock only.");
    }

    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
      throw new IOException("Blog allows write with HLC clock only.");
    }

    @Override
    public synchronized void write(HybridLogicalClock mhlc, long userTimestamp, long blockOffset, byte[] buff,
                                   int buffOffset, int len) throws IOException {
      if (len > 0)
        LOG.info("MemDatasetManager: blockOffset " + blockOffset + " len " + len + " buf " + buff[buffOffset]);
      LOG.info("HLC before blog.writeBlock: " + mhlc);
      int ret = blog.writeBlock(mhlc, userTimestamp, blockId, (int) blockOffset, buffOffset, len, buff);
      LOG.info("HLC after blog.writeBlock: " + mhlc);

      if (ret < 0) {
        throw new IOException("error in JNIBlog.write(" + mhlc + "," + userTimestamp + "," + blockId + "," +
                              blockOffset + "," + buffOffset + "," + len + ",b):" + ret);
      } else {
        offset = (int) blockOffset + len;
      }
      this.meta.accBytes += len;
    }
  }

  public MemDatasetManager(Configuration conf) {
    this.blocksize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    this.pagesize = conf.getInt(DFS_MEMBLOCK_PAGESIZE, DEFAULT_DFS_MEMBLOCK_PAGESIZE);
    this.rdmaport = conf.getInt(DFS_RDMA_CON_PORT_KEY, DFS_RDMA_CON_PORT_DEFAULT);
    this.rdmadev = conf.getTrimmed(DFS_RDMA_DEVICE_KEY, DFS_RDMA_DEVICE_DEFAULT);
    this.useRDMA = ( conf.getBoolean(DFS_CLIENT_USE_RDMA_BLOCKREADER,DFS_CLIENT_USE_RDMA_BLOCKREADER_DEFAULT) || conf.getBoolean(DFS_CLIENT_USE_RDMA_BLOCKWRITER,DFS_CLIENT_USE_RDMA_BLOCKWRITER_DEFAULT) );
    this.capacity = conf.getLong("dfs.memory.capacity", 1024 * 1024 * 1024 * 2l);
    this.blogMap = new HashMap<String, JNIBlog>();
    String[] dataDirs = conf.getTrimmedStrings(DFS_DATANODE_DATA_DIR_KEY);
    if(dataDirs.length > 0)
      this.perspath = dataDirs[0];
    else
      this.perspath = "/tmp"; // the default persistent path is in /tmp
//    this.diskMaps = new HashMap<ExtendedBlockId, String>();
  }
  
  void shutdown() {
    for (Map.Entry<String, JNIBlog> entry : this.blogMap.entrySet()) {
    	entry.getValue().destroy();
    }
  }
  
  long getCapacity() {
    return capacity;
  }
  
  /**
   * get the metadata
   * @param bpid bpid
   * @param blockId blockId
   * @return
   */
  MemBlockMeta get(String bpid, long blockId) {
    JNIBlog blog = blogMap.get(bpid);
    return (blog==null) ? null : blog.blockMaps.get(blockId);
  }
  
  private JNIBlog newJNIBlog(String bpid) {
    JNIBlog rBlog = new JNIBlog();
    // If path does not exists, create it firs.
    File fPers = new File(this.perspath + System.getProperty("file.separator") + "pers-" + bpid);
    LOG.info("pers-" + bpid);
    if (fPers.exists() && fPers.isFile())
      fPers.delete();
    if (!fPers.exists()) {
      if (fPers.mkdir() == false)
        LOG.error("Initialize Blog: cannot create path:" + fPers.getAbsolutePath());
    }

    rBlog.initialize(this, bpid, capacity, (int)blocksize, pagesize, fPers.getAbsolutePath(), useRDMA, rdmadev,
                     rdmaport);
    return rBlog;
  }
  
  JNIBlog getJNIBlog(String bpid) {
    JNIBlog blog = null;
    synchronized(blogMap) {
      blog = blogMap.get(bpid);
      if (blog == null) {
        blog = newJNIBlog(bpid);
        blogMap.put(bpid, blog);
      }
    }
    return blog;
  }
  
  /**
   * create a block
   * @param bpid
   * @param blockId
   * @param genStamp
   * @param mhlc message hybrid logical clock clock: input/output
   * @return metadata
   */
  MemBlockMeta createBlock(String bpid, long blockId, long genStamp, HybridLogicalClock mhlc) {
    JNIBlog blog = getJNIBlog(bpid);
    synchronized(blog){
      blog.createBlock(mhlc, blockId, genStamp);
      MemBlockMeta meta = new MemBlockMeta(bpid, genStamp, blockId, ReplicaState.TEMPORARY); 
      blog.blockMaps.put(blockId, meta);
      return meta;
    }
  }
  
  /**
   * delete a block:  this is used for invalidation, but keep the
   * history.
   * @param bpid
   * @param blockId
   * @param mhlc message vector clock : input/output
   */
  void deleteBlock(String bpid, long blockId, HybridLogicalClock mhlc) {
    JNIBlog blog = getJNIBlog(bpid);
    synchronized(blog){
      blog.blockMaps.get(blockId).delete();
      blog.deleteBlock(mhlc, blockId);
    }
  }
  
  /**
   * Just REALLY remove the block for clean up reason.
 * @param bpid
 * @param blockId
 */
void removeBlock(String bpid, long blockId){
    JNIBlog blog = getJNIBlog(bpid);
    blog.blockMaps.remove(blockId);
  }

  /**
   * Only return the latest block.
 * @param bpid
 * @param state
 * @return
 */
List<Block> getBlockMetas(String bpid, ReplicaState state) {
    LinkedList<Block> results = new LinkedList<Block>();
    JNIBlog blog = getJNIBlog(bpid);
    synchronized(blog){
      for(Entry<Long,MemBlockMeta> entry:blog.blockMaps.entrySet()){
        MemBlockMeta mbm = entry.getValue();
        if(!mbm.isDeleted() && (state == null || mbm.getState() == state)){
          results.add(mbm);
        }
      } 
    }
    return results;
  }

  /**
   * create a blog snapshot Id. 
 * @param bpid
 * @param snapshotId - rtc
 * @param eid
 * @throws IOException
 */
  void snapshot(String bpid, long rtc)throws IOException{
/* TODO: remove create Snapshot for datanode.
    JNIBlog blog = getJNIBlog(bpid);
    synchronized(blog){
      blog.createSnapshot(rtc);
    }
*/
  }
}
