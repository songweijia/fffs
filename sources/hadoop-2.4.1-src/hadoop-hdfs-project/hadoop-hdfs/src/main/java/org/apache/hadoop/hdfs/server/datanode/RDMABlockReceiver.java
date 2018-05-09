/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import java.io.OutputStream;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.zookeeper.common.IOUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetImpl;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWriteAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.RDMAWritePacketProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import edu.cornell.cs.sa.HybridLogicalClock;
import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.ClientTraceLog;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import edu.cornell.cs.blog.IRecordParser;
/**
 * @author weijia
 *
 */
public class RDMABlockReceiver implements Closeable {

  private final String inAddr,myAddr;
  private final int rpid;
  private DataNode datanode;
  private final String clientName;
  private final DataInputStream in;
  private final ExtendedBlock block;
  private final long vaddr;
  private final MemBlockMeta replicaInfo;
  private final LinkedList<RDMAWriteAckProto> ackQueue;
  private long lastSeqno;
  private volatile boolean isClosed;
  private Thread responderThd;
  static final Log LOG = DataNode.LOG;
  private final long startTime;
  private final IRecordParser recordParser;
  
  /**
   * constructor
   * @param block
   * @param in
   * @param inAddr
   * @param myAddr
   * @param bytesRcvd
   * @param newGs
   * @param clientName
   * @param datanode
   * @param vaddr
   * @param mhlc
   * @param rp - record parser
   * @throws IOException
   */
  RDMABlockReceiver(final ExtendedBlock block, 
      final DataInputStream in,
      final String inAddr, 
      final String myAddr,
      final int rpid,
      final long bytesRcvd,
      final long newGs, 
      final String clientName,
      final DataNode datanode, 
      final long vaddr, 
      HybridLogicalClock mhlc,
      IRecordParser rp)
  throws IOException{
    //STEP 1: initialize the receiver
    this.inAddr = inAddr;
    this.myAddr = myAddr;
    this.datanode = datanode;
    this.clientName = clientName;
    this.in = in;
    this.block = block;
    this.vaddr = vaddr;
    this.ackQueue = new LinkedList<RDMAWriteAckProto>();
    this.lastSeqno = -1L;
    this.isClosed = false;
    this.rpid = rpid;
    this.recordParser = rp;
    startTime = datanode.ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0L;
    
    
    //STEP 2: create replicaInfo
    if(datanode.data.getReplica(block)==null){
      //create new replica
      replicaInfo = (MemBlockMeta)datanode.data.createRbw(block,mhlc);
      datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
    }else{
      //open existing replica
      replicaInfo = (MemBlockMeta)datanode.data.append(block, mhlc, newGs, bytesRcvd);
      if(datanode.blockScanner!=null)
        datanode.blockScanner.deleteBlock(block.getBlockPoolId(), block.getLocalBlock());
      block.setGenerationStamp(newGs);
      datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
    }
  }
  
  class Responder implements Runnable{

    final OutputStream replyOutputStream;
    
    Responder(OutputStream out){
      this.replyOutputStream = out;
    }
    
    @Override
    public void run() {
      while(!isClosed){
        synchronized(ackQueue){
          LOG.debug("[R] lock aQ.");
          if(ackQueue.isEmpty() && !isClosed)
            try {
              LOG.debug("[R] wait on aQ.");
              ackQueue.wait();
              LOG.debug("[R] is awaken on aQ.");
              continue;
            } catch (InterruptedException e) {
              //do nothing
            }
          if(ackQueue.isEmpty()) // probably closed.
            continue;
          RDMAWriteAckProto ack = ackQueue.getFirst();
          LOG.debug("[R] get ack:"+ack);
          try {
            ack.writeDelimitedTo(replyOutputStream);
            replyOutputStream.flush();
          } catch (IOException e) {
            LOG.error("RDMABlockReceiver.Responder: Cannot send ack:"+ack);
          }
          LOG.debug("[R] sends ack:"+ack);
          ackQueue.removeFirst();
          LOG.debug("[R] removed ack:"+ack);
          ackQueue.notifyAll();
          LOG.debug("[R] kicks aQ");
        }
      }
    }
  }
  
  boolean receiveNextPacket()throws IOException{
    long ts = System.nanoTime(),ts1,ts2,ts3,ts4,ts5;
    LOG.debug("[S] waits on pkt.");
    RDMAWritePacketProto proto = RDMAWritePacketProto.parseFrom(vintPrefixed(in));
    ts1 = System.nanoTime();
    LOG.debug("[S] gets packet:seqno="+proto.getSeqno());
    long seqno = proto.getSeqno();
    boolean islast = proto.getIsLast();
    long length = proto.getLength();
    long offset = proto.getOffset();
    boolean bRet = true;
    HybridLogicalClock mhlc = PBHelper.convert(proto.getMhlc());
    //STEP 0: validate:
    if(seqno != -1L && lastSeqno != -1L && seqno != lastSeqno + 1){
      throw new IOException("RDMABlockReceiver out-of-order packet received: expecting seqno[" +
        (lastSeqno+1) + "] but received seqno["+seqno+"]");
    }
    //STEP 1: handle normal write or finalize block before we enqueue the ack.
    ts2 = System.nanoTime();
    if(seqno >= 0L){
      replicaInfo.writeByRDMA((int)offset, (int)length, this.inAddr, this.rpid, this.vaddr, mhlc, this.recordParser);
      LOG.debug("[S] replicaInfo.length="+replicaInfo.getNumBytes()+"/"+replicaInfo.getBytesOnDisk());
      lastSeqno = seqno;
      LOG.debug("[S] wrote packet:offset="+offset+",length="+length+",peer="+this.inAddr+",vaddr="+vaddr);
    } else if(islast){
      finalizeBlock(this.startTime);
      LOG.debug("[S] finalized block:"+replicaInfo);
    }
    ts3 = System.nanoTime();
    //STEP 2: enqueue ack...
    RDMAWriteAckProto ack = RDMAWriteAckProto.newBuilder()
        .setSeqno(seqno)
        .setStatus(Status.SUCCESS)
        .setMhlc(PBHelper.convert(mhlc))
        .build();
    synchronized(this.ackQueue){
      LOG.debug("[S] locks aQ.");
      this.ackQueue.addLast(ack);
      LOG.debug("[S] adds ack:seqno="+ack.getSeqno());
      ackQueue.notifyAll();
      LOG.debug("[S] kicks aQ.");
    }
    ts4 = System.nanoTime();

    //STEP 3: wait and stop.
    if(seqno == -1L && islast){
      LOG.debug("[S] waits until aQ is empty.");
      synchronized(this.ackQueue){
        while(!this.ackQueue.isEmpty()){
          try {
            this.ackQueue.wait();
          } catch (InterruptedException e) {
            //do nothing
          }
        }
        isClosed = true;
        this.ackQueue.notifyAll();
      }
      LOG.debug("[S] found aQ is empty, return with false");
      bRet = false;
    }
    ts5 = System.nanoTime();

    // SEQNO RECV WRITE/FINAL ENQ-ACK FLUSH
    //LOG.error(seqno+" "+(ts2-ts1)+" "+(ts3-ts2)+" "+(ts4-ts3)+" "+(ts5-ts4));
    
    return bRet;
  }
  
  private void finalizeBlock(long startTime) throws IOException {
    
    final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0L;
    block.setNumBytes(replicaInfo.getNumBytes());
    datanode.data.finalizeBlock(block);
    datanode.closeBlock(block, DataNode.EMPTY_DEL_HINT, replicaInfo.getStorageUuid());
    if(ClientTraceLog.isInfoEnabled()){
      long offset = 0;
      DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block.getBlockPoolId());
      ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
          myAddr, block.getNumBytes(), "HDFS_WRITE", clientName, offset,
          dnR.getDatanodeUuid(), block, endTime - startTime));
    } else {
      LOG.info("Received " + block + " size " + block.getNumBytes()
      + " from " + inAddr);
    }
  }
  
  /**
   * Receive all packages to a block
   * @param replyOut
   */
  void receiveBlock(DataOutputStream replyOut)throws IOException{
    this.isClosed = false;
    //STEP 1: run responder. 
    LOG.debug("[S] receiveBlock is called.");
    this.responderThd = new Thread(new Responder(replyOut));
    this.responderThd.start();
    LOG.debug("[S] Responder is started.");
    //STEP 2: do receive packet.
    while(receiveNextPacket());
    try {
      LOG.debug("[S] waits until responder finished.");
      this.responderThd.join();
    } catch (InterruptedException e) {
      LOG.info("RDMABlockReceiver: fail to join responder thread.");
    }
    replyOut.flush();
  }
  
  @Override
  public void close() throws IOException {
    // do nothing here...
  }

}
