package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

import edu.cornell.cs.blog.JNIBlog;
import edu.cornell.cs.sa.HybridLogicalClock;

//@InterfaceAudience.Private
public class MemDatasetImpl implements FsDatasetSpi<MemVolumeImpl> {
  static final Log LOG = LogFactory.getLog(MemDatasetImpl.class);

  @Override // FsDatasetSpi
  public List<MemVolumeImpl> getVolumes() {
    return volumes;
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override // FsDatasetSpi
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    StorageReport[] reports;
    synchronized (statsLock) {
      reports = new StorageReport[volumes.size()];
      int i = 0;
      for (MemVolumeImpl volume : volumes) {
        reports[i++] = new StorageReport(volume.toDatanodeStorage(),
                                         false,
                                         volume.getCapacity(),
                                         volume.getDfsUsed(),
                                         volume.getAvailable(),
                                         volume.getBlockPoolUsed(bpid));
      }
    }

    return reports;
  }

  @Override
  public synchronized MemVolumeImpl getVolume(final ExtendedBlock b) {
    return volumes.get(0);
  }

  @Override // FsDatasetSpi
  public synchronized Block getStoredBlock(ExtendedBlock b)
      throws IOException {
    MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (meta == null) return null;
    return meta;
  }
  
  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    return null;
  }
    
  final DataNode datanode;
  final DataStorage dataStorage;
  final List<MemVolumeImpl> volumes;
  final Map<String, DatanodeStorage> storageMap;
  final MemDatasetManager memManager;
  private final int validVolsRequired;

  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

  /**
   * An FSDataset has a directory where it loads its data files.
   */
  MemDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {
    this.datanode = datanode;
    this.dataStorage = storage;
    this.validVolsRequired = 1;
    
//    memManager = new MemDatasetManager(this, conf);
    memManager = new MemDatasetManager(conf);
    
    storageMap = new HashMap<String, DatanodeStorage>();
    storageMap.put("0", new DatanodeStorage("0", DatanodeStorage.State.NORMAL, StorageType.MEM));
    volumes = new ArrayList<MemVolumeImpl>(1);
    // use the first dir in Storage
    final File dir = storage.getStorageDir(0).getCurrentDir();
    volumes.add(new MemVolumeImpl(this, "0", dir, conf, StorageType.MEM));

    registerMBean(datanode.getDatanodeUuid());
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    synchronized(statsLock) {
      return volumes.get(0).getDfsUsed();
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    synchronized(statsLock) {
      return volumes.get(0).getBlockPoolUsed(bpid);
    }
  }
  
  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    return getVolumes().size() >= validVolsRequired; 
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() {
    synchronized(statsLock) {
      return volumes.get(0).getCapacity();
    }
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    synchronized(statsLock) {
      return volumes.get(0).getAvailable();
    }
  }

  /**
   * Return the number of failed volumes in the FSDataset.
   */
  @Override
  public int getNumFailedVolumes() {
    return 0;
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FsDatasetSpi
  public long getLength(ExtendedBlock b) throws IOException {
    memManager.get(b.getBlockPoolId(), b.getBlockId());
    MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (meta != null) return meta.getNumBytes();
    return -1;
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {
    return getBlockInputStream(b,seekOffset, -1L, false);
  }
  
  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset, long timestamp, boolean bUserTimestamp ) throws IOException {
    MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if(meta != null)
      return meta.getInputStream((int)seekOffset, timestamp, bUserTimestamp);
    else return null;
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  @Override // FsDatasetSpi
  public synchronized ReplicaInputStreams getTmpInputStreams(ExtendedBlock b, 
                          long blkOffset, long ckoff) throws IOException {
    return null;
  }

  @Override  // FsDatasetSpi
  public synchronized Replica append(ExtendedBlock b, HybridLogicalClock mhlc,
      long newGS, long expectedBlockLen) throws IOException {
    // If the block was successfully finalized because all packets
    // were successfully processed at the Datanode but the ack for
    // some of the packets were not received by the client. The client 
    // re-opens the connection and retries sending those packets.
    // The other reason is that an "append" is occurring to this block.
    
    // check the validity of the parameter
    if(b.getLocalBlock().getLongSid() != JNIBlog.CURRENT_SNAPSHOT_ID){
      throw new IOException("Cannot append to snapshot");
    }
    if (newGS < b.getGenerationStamp()) {
      throw new IOException("The new generation stamp " + newGS + 
          " should be greater than the replica " + b + "'s generation stamp");
    }
    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    LOG.info("Appending to block " + replicaInfo.getBlockId());
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
    }
    if (replicaInfo.getNumBytes() != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo.getBlockId() + 
          " with a length of " + replicaInfo.getNumBytes() + 
          " expected length is " + expectedBlockLen);
    }

    return append(mhlc, b.getBlockPoolId(), replicaInfo, newGS,
        b.getNumBytes());
  }
  
  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param mhlc message hybrid clock
   * @param bpid block pool Id
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate generation stamp
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private synchronized Replica append(HybridLogicalClock mhlc,String bpid,
      MemDatasetManager.MemBlockMeta replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {
    // If the block is cached, start uncaching it.
    //cacheManager.uncacheBlock(bpid, replicaInfo.getBlockId());
    // unlink the finalized replica
    // replicaInfo.unlinkBlock(1);
    
    // construct a RBW replica with the new GS
    MemVolumeImpl v = volumes.get(0);
    if (v.getAvailable() < estimateBlockLen - replicaInfo.getNumBytes()) {
      throw new DiskOutOfSpaceException("Insufficient space for appending to block "
          + replicaInfo.getBlockId());
    }

    replicaInfo.setGenerationStamp(mhlc, newGS);
    replicaInfo.setState(ReplicaState.RBW);

    return replicaInfo;
  }

  private MemDatasetManager.MemBlockMeta recoverCheck(ExtendedBlock b, long newGS, 
      long expectedBlockLen) throws IOException {
    if(b.getLocalBlock().getLongSid() != JNIBlog.CURRENT_SNAPSHOT_ID){
    	throw new IOException("Cannot recover snapshot block");
    }
    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    
    // check state
    if (replicaInfo.getState() != ReplicaState.FINALIZED &&
        replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA + replicaInfo.getBlockId());
    }

    // check generation stamp
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + replicaGenerationStamp
          + ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }
    
    // check block length
    if (replicaInfo.getNumBytes() != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo + 
          " with a length of " + replicaInfo.getNumBytes() + 
          " expected length is " + expectedBlockLen);
    }
    
    return replicaInfo;
  }
  
  @Override  // FsDatasetSpi
  public synchronized Replica recoverAppend(ExtendedBlock b, HybridLogicalClock mhlc,
      long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    MemDatasetManager.MemBlockMeta replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    // change the replica's state/gs etc.
    if (replicaInfo.getState() == ReplicaState.FINALIZED ) {
      return append(mhlc, b.getBlockPoolId(), replicaInfo, newGS, 
          b.getNumBytes());
    } else { //RBW
      replicaInfo.setGenerationStamp(newGS);
      return replicaInfo;
    }
  }

  @Override // FsDatasetSpi
  public String recoverClose(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    LOG.info("Recover failed close " + b);
    // check replica's state
    MemDatasetManager.MemBlockMeta replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
    // bump the replica's GS
    replicaInfo.setGenerationStamp(newGS);
    // finalize the replica if RBW
    if (replicaInfo.getState() == ReplicaState.RBW) {
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
    return replicaInfo.getStorageUuid();
  }
/*
  @Override // FsDatasetSpi
  public synchronized Replica createRbw(ExtendedBlock b)
      throws IOException {
    Replica meta = memManager.get(b.getBlockPoolId() + b.getLocalBlock().getSid(), b.getBlockId());
    if (meta != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists and thus cannot be created.");
    }
    // create a new block
    return memManager.getNewBlock(b.getBlockPoolId() + b.getLocalBlock().getSid(), b.getBlockId(), b.getGenerationStamp());
  }
*/  
  @Override // FsDatasetSpi
  public synchronized Replica recoverRbw(ExtendedBlock b,
      long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    LOG.info("Recover RBW replica " + b);
    
    if(b.getLocalBlock().getLongSid() != JNIBlog.CURRENT_SNAPSHOT_ID)
    	throw new IOException("Cannot recover snapshot.");

    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    
    // check the replica's state
    if (replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
    }
    
    LOG.info("Recovering " + replicaInfo.getBlockId());

    // check generation stamp
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
          ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }
    
    // check replica length
    if (replicaInfo.getNumBytes() > maxBytesRcvd){
      throw new ReplicaNotFoundException("Unmatched length replica " + 
          replicaInfo + ": BytesRcvd = " + replicaInfo.getNumBytes() + " are not in the range of [" + 
          minBytesRcvd + ", " + maxBytesRcvd + "].");
    }

    // bump the replica's generation stamp to newGS
    replicaInfo.setGenerationStamp(newGS);
    return replicaInfo;
  }
  
  @Override // FsDatasetSpi
  public synchronized Replica convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {
    if(b.getLocalBlock().getLongSid()!=JNIBlog.CURRENT_SNAPSHOT_ID)
      throw new IOException("Cannot change states of snapshot replica.");
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    LOG.info("Convert " + b + " from Temporary to RBW, visible length="
        + visible);

    MemDatasetManager.MemBlockMeta r = memManager.get(b.getBlockPoolId(),
    		b.getBlockId());
 
    if (r == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
    }
    // check the replica's state
    if (r.getState() != ReplicaState.TEMPORARY) {
      throw new ReplicaAlreadyExistsException(
          "r.getState() != ReplicaState.TEMPORARY, r=" + r);
    }

    // check generation stamp
    if (r.getGenerationStamp() != expectedGs) {
      throw new ReplicaAlreadyExistsException(
          "temp.getGenerationStamp() != expectedGs = " + expectedGs
          + ", temp=" + r.getBlockId());
    }

    // set writer to the current thread
    // temp.setWriter(Thread.currentThread());

    // check length
    final long numBytes = r.getNumBytes();
    if (numBytes < visible) {
      throw new IOException(numBytes + " = numBytes < visible = "
          + visible + ", temp=" + r.getBlockId());
    }

    r.setState(ReplicaState.RBW);
    return r;
  }
/*
  @Override // FsDatasetSpi
  public synchronized Replica createTemporary(ExtendedBlock b)
      throws IOException {
    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId() + b.getLocalBlock().getSid(), b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " already exists in state " + replicaInfo.getState() +
          " and thus cannot be created.");
    }
    
    return memManager.getNewBlock(b.getBlockPoolId() + b.getLocalBlock().getSid(), b.getBlockId(), b.getGenerationStamp());
  }
*/
  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  @Override // FsDatasetSpi
  public void adjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams streams, 
      int checksumSize) throws IOException {}

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  @Override // FsDatasetSpi
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }
    
    if(b.getLocalBlock().getLongSid() != JNIBlog.CURRENT_SNAPSHOT_ID)
    	throw new IOException("in finalizeBlock(): Cannot finalize snapshot replica.");
    
    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      // this is legal, when recovery happens on a file that has
      // been opened for append but never modified
      return;
    }
    finalizeReplica(b.getBlockPoolId(), replicaInfo);
  }
  
  private synchronized Replica finalizeReplica(String bpid,
      MemDatasetManager.MemBlockMeta replicaInfo) throws IOException {
    if (replicaInfo.getState() != ReplicaState.RUR) {
      MemVolumeImpl v = volumes.get(0);
      if (v == null) {
        throw new IOException("No volume for block " + replicaInfo);
      }
      v.incDfsUsed(bpid, replicaInfo.accBytes);
      replicaInfo.accBytes = 0l;
    }
    replicaInfo.setState(ReplicaState.FINALIZED);
    return replicaInfo;
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FsDatasetSpi
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException {
    if(b.getLocalBlock().getLongSid()!=JNIBlog.CURRENT_SNAPSHOT_ID)
      throw new IOException("cannot unfinalize snapshoted block");
    MemDatasetManager.MemBlockMeta replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (replicaInfo != null && replicaInfo.getState() == ReplicaState.TEMPORARY) {
      memManager.removeBlock(b.getBlockPoolId(), b.getBlockId());
    }
  }

  @Override
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {
    Map<DatanodeStorage, BlockListAsLongs> blockReportsMap =
        new HashMap<DatanodeStorage, BlockListAsLongs>();

    Map<String, ArrayList<Block>> finalized =
        new HashMap<String, ArrayList<Block>>();
    Map<String, ArrayList<Block>> uc =
        new HashMap<String, ArrayList<Block>>();

    for (FsVolumeSpi v : volumes) {
      finalized.put(v.getStorageID(), new ArrayList<Block>());
      uc.put(v.getStorageID(), new ArrayList<Block>());
    }

    synchronized(this) {
      for (Block b : memManager.getBlockMetas(bpid, null)) {
        switch(((MemDatasetManager.MemBlockMeta)b).getState()) {
          case FINALIZED:
            finalized.get("0").add(b);
            break;
          case RBW:
          case RWR:
          case RUR:
            uc.get("0").add(b);
            break;
          case TEMPORARY:
            break;
          default:
            assert false : "Illegal ReplicaInfo state.";
        }
      }
    }

    for (MemVolumeImpl v : volumes) {
      ArrayList<Block> finalizedList = finalized.get(v.getStorageID());
      ArrayList<Block> ucList = uc.get(v.getStorageID());
      blockReportsMap.put(v.toDatanodeStorage(),
                          new BlockListAsLongs(finalizedList, ucList));
    }

    return blockReportsMap;
  }

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    //return cacheManager.getCachedBlocks(bpid);
    return new ArrayList<Long>();
  }

  /**
   * Get the list of finalized blocks from in-memory blockmap for a block pool.
   */
  @Override
  public synchronized List<Block> getFinalizedBlocks(String bpid) {
    return memManager.getBlockMetas(bpid, ReplicaState.FINALIZED);
  }

  /**
   * Check whether the given block is a valid one.
   * valid means finalized
   */
  @Override // FsDatasetSpi
  public boolean isValidBlock(ExtendedBlock b) {
    return isValid(b, ReplicaState.FINALIZED);
  }

  /**
   * Check whether the given block is a valid RBW.
   */
  @Override // {@link FsDatasetSpi}
  public boolean isValidRbw(final ExtendedBlock b) {
    return isValid(b, ReplicaState.RBW);
  }

  /** Does the block exist and have the given state? */
  private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
    final Replica replicaInfo = memManager.get(b.getBlockPoolId(), b.getBlockId());
    return replicaInfo != null
        && replicaInfo.getState() == state;
  }

  
  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
/*
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      final MemVolumeImpl v = volumes.get(0);
      synchronized (this) {
        final MemDatasetManager.MemBlockMeta info = memManager.get(bpid + invalidBlks[i].getSid(), invalidBlks[i].getBlockId());
        if (info == null) {
          // It is okay if the block is not found -- it may be deleted earlier.
          LOG.info("Failed to delete replica " + invalidBlks[i]
              + ": ReplicaInfo not found.");
          continue;
        }
        if (info.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              + ": GenerationStamp not matched, info=" + info);
          continue;
        }

        ReplicaState replicaState = info.getState();
        if (replicaState == ReplicaState.FINALIZED || 
            replicaState == ReplicaState.RUR) {
          v.decDfsUsed(bpid, info.getNumBytes());
        }
        memManager.deleteBlock(bpid + invalidBlks[i].getSid(), invalidBlks[i].getBlockId());
      }
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
        .append(errors.size()).append(" (out of ").append(invalidBlks.length)
        .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }
*/
  @Override // FsDatasetSpi
  public synchronized boolean contains(final ExtendedBlock block) {
    return memManager.get(block.getBlockPoolId(), block.getBlockId()) != null;
  }

  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   * @throws DiskErrorException
   */
  @Override // FsDatasetSpi
  public void checkDataDir() throws DiskErrorException {
    return;
  }
    

  @Override // FsDatasetSpi
  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
   */
  void registerMBean(final String datanodeUuid) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    try {
      StandardMBean bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + datanodeUuid, bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering FSDatasetState MBean", e);
    }
    LOG.info("Registered FSDatasetState MBean");
  }

  @Override // FsDatasetSpi
  public void shutdown() {
    if (mbeanName != null)
      MBeans.unregister(mbeanName);
    memManager.shutdown();
  }

  @Override // FSDatasetMBean
  public String getStorageInfo() {
    return toString();
  }

  /**
   * Reconcile the difference between blocks on the disk and blocks in
   * volumeMap
   *
   * Check the given block for inconsistencies. Look at the
   * current state of the block and reconcile the differences as follows:
   * <ul>
   * <li>If the block file is missing, delete the block from volumeMap</li>
   * <li>If the block file exists and the block is missing in volumeMap,
   * add the block to volumeMap <li>
   * <li>If generation stamp does not match, then update the block with right
   * generation stamp</li>
   * <li>If the block length in memory does not match the actual block file length
   * then mark the block as corrupt and update the block length in memory</li>
   * <li>If the file in {@link ReplicaInfo} does not match the file on
   * the disk, update {@link ReplicaInfo} with the correct file</li>
   * </ul>
   *
   * @param blockId Block that differs
   * @param diskFile Block file on the disk
   * @param diskMetaFile Metadata file from on the disk
   * @param vol Volume of the block file
   */
  @Override
  public void checkAndUpdate(String bpid, long blockId, File diskFile,
      File diskMetaFile, FsVolumeSpi vol) {
    return;
  }

  /**
   * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
   */
  @Override // FsDatasetSpi
  //@Deprecated
  public Replica getReplica(ExtendedBlock b) {
    return memManager.get(b.getBlockPoolId(), b.getBlockId());
  }

  @Override 
  public synchronized String getReplicaString(ExtendedBlock b) {
    final Replica r = memManager.get(b.getBlockPoolId(), b.getBlockId());
    return r == null? "null": r.toString();
  }
  @Override // FsDatasetSpi
  public synchronized ReplicaRecoveryInfo initReplicaRecovery(
      RecoveringBlock rBlock) throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), 
        memManager.get(rBlock.getBlock().getBlockPoolId() + rBlock.getBlock(), rBlock.getBlock().getBlockId()),
        rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(),
        datanode.getDnConf().getXceiverStopTimeout());
  }

  /** static version of {@link #initReplicaRecovery(Block, long)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid, Replica replica,
      Block block, long recoveryId, long xceiverStopTimeout) throws IOException {
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      return null;
    }
    
    //check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }

    //check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getGenerationStamp() >= recoveryId = " + recoveryId
          + ", block=" + block + ", replica=" + replica);
    }

    if (replica.getState() != ReplicaState.RUR) {
      ((MemDatasetManager.MemBlockMeta)replica).setState(ReplicaState.RUR);
    }
    return null;
  }

  @Override // FsDatasetSpi
  public synchronized String updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
                                    final long newlength) throws IOException {
    //get replica
    final MemDatasetManager.MemBlockMeta replica = memManager.get(oldBlock.getBlockPoolId(), oldBlock.getBlockId());
    LOG.info("updateReplica: " + oldBlock
        + ", recoveryId=" + recoveryId
        + ", length=" + newlength
        + ", replica=" + replica);

    //check replica
    if (replica == null) {
      throw new ReplicaNotFoundException(oldBlock);
    }

    //check replica state
    if (replica.getState() != ReplicaState.RUR) {
      throw new IOException("replica.getState() != " + ReplicaState.RUR
          + ", replica=" + replica);
    }

    //check replica's byte on disk
    if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getBytesOnDisk() != block.getNumBytes(), block="
          + oldBlock + ", replica=" + replica);
    }
//TODO
    //update replica
    final Replica finalized = updateReplicaUnderRecovery(oldBlock
        .getBlockPoolId(), replica, recoveryId, newlength);
    assert finalized.getBlockId() == oldBlock.getBlockId()
        && finalized.getGenerationStamp() == recoveryId
        && finalized.getNumBytes() == newlength
        : "Replica information mismatched: oldBlock=" + oldBlock
            + ", recoveryId=" + recoveryId + ", newlength=" + newlength
            + ", finalized=" + finalized;

    //return storage ID
    return volumes.get(0).getStorageID();
  }

  private Replica updateReplicaUnderRecovery(
                                          String bpid,
                                          MemDatasetManager.MemBlockMeta rur,
                                          long recoveryId,
                                          long newlength) throws IOException {
    // bump rur's GS to be recovery id
    rur.setGenerationStamp(recoveryId);

    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {      
      // update RUR with the new length
      rur.setNumBytes(newlength);
   }

    // finalize the block
    return finalizeReplica(bpid, rur);
  }

  @Override // FsDatasetSpi
  public synchronized long getReplicaVisibleLength(final ExtendedBlock block)
  throws IOException {
    final MemDatasetManager.MemBlockMeta replica = memManager.get(block.getBlockPoolId(), block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }
  
  @Override // FsDatasetSpi
  public synchronized long getReplicaVisibleLength(final ExtendedBlock block,
      long timestamp,boolean bUserTimestamp)
  throws IOException {
    final MemDatasetManager.MemBlockMeta replica = memManager.get(block.getBlockPoolId(), block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }
    return replica.getVisibleLength(timestamp,bUserTimestamp);
  }
  
  @Override
  public void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    LOG.info("Adding block pool " + bpid);
    synchronized(this) {
      volumes.get(0).addBlockPool(bpid, conf);
    }
  }

  @Override
  public synchronized void shutdownBlockPool(String bpid) {
    LOG.info("Removing block pool " + bpid);
    volumes.get(0).shutdownBlockPool(bpid);
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace;
    final long freeSpace;
    final long reservedSpace;

    VolumeInfo(MemVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (MemVolumeImpl volume : volumes) {
      long used = 0;
      long free = 0;
      try {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        used = 0;
        free = 0;
      }
      
      info.add(new VolumeInfo(volume, used, free));
    }
    return info;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      info.put(v.directory, innerInfo);
    }
    return info;
  }

  @Override //FsDatasetSpi
  public synchronized void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    if (!force) {
      for (MemVolumeImpl volume : volumes) {
        if (volume.getBlockPoolUsed(bpid) > 0) {
          LOG.warn(bpid + " has some block files, cannot delete unless forced");
          throw new IOException("Cannot delete block pool, "
              + "it contains some block files");
        }
      }
    }
    for (MemVolumeImpl volume : volumes) {
      volume.shutdownBlockPool(bpid);
    }
  }
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    return null;
  }

  @Override // FsDatasetSpi
  public HdfsBlocksMetadata getHdfsBlocksMetadata(String poolId,
      long[] blockIds) throws IOException {
    // List of VolumeIds, one per volume on the datanode
    List<byte[]> blocksVolumeIds = new ArrayList<byte[]>(volumes.size());
    // List of indexes into the list of VolumeIds, pointing at the VolumeId of
    // the volume that the block is on
    List<Integer> blocksVolumeIndexes = new ArrayList<Integer>(blockIds.length);
    // Initialize the list of VolumeIds simply by enumerating the volumes
    for (int i = 0; i < volumes.size(); i++) {
      blocksVolumeIds.add(ByteBuffer.allocate(4).putInt(i).array());
    }
    // Determine the index of the VolumeId of each block's volume, by comparing 
    // the block's volume against the enumerated volumes
    for (int i = 0; i < blockIds.length; i++) {
      blocksVolumeIndexes.add(memManager.get(poolId, blockIds[i]) == null? Integer.MAX_VALUE: 0);
    }
    return new HdfsBlocksMetadata(poolId, blockIds,
        blocksVolumeIds, blocksVolumeIndexes);
  }

  @Override
  public void enableTrash(String bpid) {
    dataStorage.enableTrash(bpid);
  }

  @Override
  public void restoreTrash(String bpid) {
    dataStorage.restoreTrash(bpid);
  }

  @Override
  public boolean trashEnabled(String bpid) {
    return dataStorage.trashEnabled(bpid);
  }

  @Override
  public RollingLogs createRollingLogs(String bpid, String prefix
      ) throws IOException {
    String dir = volumes.get(0).getPath(bpid);
    return new RollingLogsImpl(dir, prefix);
  }

  @Override
  public long getCacheUsed() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getCacheCapacity() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getNumBlocksCached() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getNumBlocksFailedToCache() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getNumBlocksFailedToUncache() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void cache(String bpid, long[] blockIds) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void uncache(String bpid, long[] blockIds) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isCached(String bpid, long blockId) {
    // TODO Auto-generated method stub
    return false;
  }
  
  public StorageType getStorageType() {
    return StorageType.MEM;
  }
  
  public OutputStream getBlockOutputStream(ExtendedBlock b, long seekOffset) throws IOException {
    if (b.getLocalBlock().getLongSid() != JNIBlog.CURRENT_SNAPSHOT_ID)
      throw new IOException("in getBlockOutputStream(): Cannot write to snapshot.");

    MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (meta != null)
      return meta.getOutputStream((int)seekOffset);
    return null;
  }

  @Override
  public void snapshot(long rtc, String bpid)
      throws IOException {
  	memManager.snapshot(bpid, rtc);
  }
  
  @Override
  public Replica createTemporary(ExtendedBlock b, HybridLogicalClock mhlc)
	throws IOException {
	MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (meta != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists and thus cannot be created.");
    }
    // create a new block
    meta = memManager.createBlock(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp(), mhlc);
    meta.setState(ReplicaState.TEMPORARY);
    return meta;
  }

  @Override
  public Replica createRbw(ExtendedBlock b, HybridLogicalClock mhlc) throws IOException {
    MemDatasetManager.MemBlockMeta meta = memManager.get(b.getBlockPoolId(), b.getBlockId());
    if (meta != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
      " already exists and thus cannot be created.");
    }
    // create a new block
    meta = memManager.createBlock(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp(), mhlc);
    meta.setState(ReplicaState.RBW);
    return meta;
  }

  @Override
  public void invalidate(String bpid, Block[] invalidBlks, HybridLogicalClock mhlc)
	throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      if(invalidBlks[i].getLongSid()!=JNIBlog.CURRENT_SNAPSHOT_ID)continue; // do not invalid snapshot blocks.
      final MemVolumeImpl v = volumes.get(0);
      synchronized (this) {
        final MemDatasetManager.MemBlockMeta info = memManager.get(bpid, invalidBlks[i].getBlockId());
        if (info == null) {
          // It is okay if the block is not found -- it may be deleted earlier.
          LOG.info("Failed to delete replica " + invalidBlks[i]
              + ": ReplicaInfo not found.");
          continue;
        }
        if (info.getGenerationStamp() != invalidBlks[i].getGenerationStamp()) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              + ": GenerationStamp not matched, info=" + info);
          continue;
        }
	        ReplicaState replicaState = info.getState();
        if (replicaState == ReplicaState.FINALIZED || 
            replicaState == ReplicaState.RUR) {
          v.decDfsUsed(bpid, info.getNumBytes());
        }
        memManager.deleteBlock(bpid, invalidBlks[i].getBlockId(), mhlc);
      }
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
        .append(errors.size()).append(" (out of ").append(invalidBlks.length)
        .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }
  
  public MemDatasetManager getManager(){
    return this.memManager;
  }
}
