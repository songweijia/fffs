/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.primitives.Ints;
import edu.cornell.cs.blog.DefaultRecordParser;
import edu.cornell.cs.blog.IRecordParser;
import edu.cornell.cs.blog.IRecordParser.RecordParserException;
import edu.cornell.cs.perf.PerformanceTraceSwitch;
import edu.cornell.cs.sa.HybridLogicalClock;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.HLCOutputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.util.Time.now;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class MemBlockReceiver extends BlockReceiver {
  private static final List<ByteBuffer> bufferPool = new LinkedList<ByteBuffer>();
  // private static final int MAX_BUFFER_SIZE = new Configuration().getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT) + 4096;
  
  /** Replica to write */
  private MemDatasetManager.MemBlockMeta replicaInfo;
  private IRecordParser rp;
  private BlockWriter blockWriter;

  MemBlockReceiver(final ExtendedBlock block, final DataInputStream in, final String inAddr, final String myAddr,
                   final BlockConstructionStage stage, final long newGs, final long minBytesRcvd,
                   final long maxBytesRcvd, final String clientname, final DatanodeInfo srcDataNode,
                   final DataNode datanode, DataChecksum requestedChecksum, long offset/*HDFSRS_RWAPI*/,
                   HybridLogicalClock mhlc/*HDFSRS_HLC*/, IRecordParser rp) throws IOException {
    super(block, in, inAddr, myAddr, stage, clientname, srcDataNode, datanode, requestedChecksum);
    this.rp = (rp == null) ? new DefaultRecordParser() : rp;
    // this.getBuf();
  
    // Open local disk out
    if (isDatanode) {   //replication or move
      replicaInfo = (MemDatasetManager.MemBlockMeta) datanode.data.createTemporary(block, mhlc);
    } else {
      switch (stage) {
      case PIPELINE_SETUP_CREATE:
        replicaInfo = (MemDatasetManager.MemBlockMeta) datanode.data.createRbw(block, mhlc);
        datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
        break;
      case PIPELINE_SETUP_STREAMING_RECOVERY:
        replicaInfo = (MemDatasetManager.MemBlockMeta) datanode.data.recoverRbw(block, newGs, minBytesRcvd,
                                                                                maxBytesRcvd);
        block.setGenerationStamp(newGs);
        break;
      case PIPELINE_SETUP_APPEND:
      case PIPELINE_SETUP_OVERWRITE:
        replicaInfo = (MemDatasetManager.MemBlockMeta) datanode.data.append(block, mhlc, newGs, minBytesRcvd);
        if (datanode.blockScanner != null) { // remove from block scanner
          datanode.blockScanner.deleteBlock(block.getBlockPoolId(), block.getLocalBlock());
        }
        block.setGenerationStamp(newGs);
        datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
        break;
      case PIPELINE_SETUP_APPEND_RECOVERY:
        replicaInfo = (MemDatasetManager.MemBlockMeta) datanode.data.recoverAppend(block, mhlc, newGs, minBytesRcvd);
        if (datanode.blockScanner != null) { // remove from block scanner
          datanode.blockScanner.deleteBlock(block.getBlockPoolId(), block.getLocalBlock());
        }
        block.setGenerationStamp(newGs);
        datanode.notifyNamenodeReceivingBlock(block, replicaInfo.getStorageUuid());
        break;
      case TRANSFER_RBW:
      case TRANSFER_FINALIZED:
        // this is a transfer destination
        replicaInfo = (MemDatasetManager.MemBlockMeta)datanode.data.createTemporary(block,mhlc);
        break;
      default: throw new IOException("Unsupported stage " + stage + " while receiving block " + block +
                                     " from " + inAddr);
      }
    }

    try {
      this.out = datanode.data.getBlockOutputStream(block, offset);
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      throw ioe;
    }
  }

  boolean bClosed = false;

  /**
   * close files.
   */
  @Override
  public void close() throws IOException {
    if (bClosed)
      return;
    if (packetReceiver != null) {
      packetReceiver.close();
    }
    
    this.returnDataBufToPool();
    
    if (syncOnClose && out != null) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;

    // close block file
    IOException ioe = null;
    try {
      if (out != null) {
        long flushStartNanos = System.nanoTime();
        out.flush();
        long flushEndNanos = System.nanoTime();
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    } finally {
      IOUtils.closeStream(out);
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    
    if (ioe != null) {
      throw ioe;
    }
    
    bClosed = true;
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    if (out != null) {
      long flushStartNanos = System.nanoTime();
      out.flush();
      long flushEndNanos = System.nanoTime();
      flushTotalNanos += flushEndNanos - flushStartNanos;
      
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
    	  datanode.metrics.incrFsyncCount();      
      }
    }
  }

//  private static final int MAX_PACKET_SIZE = 64 * 1024 * 1024;
//  private static final int MAX_PACKET_SIZE = 3 * 1024 * 1024; // initialze a 64MB buffer takes 30 milliseconds on an old server to 3MB
  private PacketHeader header = new PacketHeader();
//  private byte[] dataBuf = new byte[MAX_PACKET_SIZE];
  private int icb = 0;
  private ByteBuffer buffers[] = new ByteBuffer[2];
  private long packetRecvTime;

  /*
  private void getBuf() {
    // Realloc the buffer if this packet is longer than the previous one.
    for (int i = 0; i < 2; i++) {
      if (buffers[i] == null) {
        ByteBuffer newBuf;
        synchronized(bufferPool) {
          if (bufferPool.size() == 0)
            newBuf = null;
          else
            newBuf = bufferPool.remove(0);
        }
        if(newBuf == null)
          newBuf = ByteBuffer.allocate(MAX_BUFFER_SIZE);
        buffers[i] = newBuf;
      }
    }
  }*/
  
  private void returnDataBufToPool() {
    for (int i = 0; i < 2; i++) {
      if (buffers[i] != null) {
        synchronized(bufferPool){
          bufferPool.add(buffers[i]);
        }
        buffers[i] = null;
      }
    }
  }
  
  /*
  void receiveNextPacket(DataInputStream in) throws IOException {
    LOG.info("Receive next packet.\n");
    int payloadLen = in.readInt();
    
    if (payloadLen < Ints.BYTES) {
      // The "payload length" includes its own length. Therefore it
      // should never be less than 4 bytes
      throw new IOException("Invalid payload length " + payloadLen);
    }
    
    int dataPlusChecksumLen = payloadLen - Ints.BYTES;
    int headerLen = in.readShort();

    if (headerLen < 0) {
      throw new IOException("Invalid header length " + headerLen);
    }
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("readNextPacket: dataPlusChecksumLen = " + dataPlusChecksumLen + " headerLen = " + headerLen);
    }
    
    byte[] headerBuf = new byte[headerLen];
    in.readFully(headerBuf);
    header.setFieldsFromData(dataPlusChecksumLen, headerBuf);
    
    int checksumLen = dataPlusChecksumLen - header.getDataLen();
    if (checksumLen < 0) {
      throw new IOException("Invalid packet: data length in packet header " + 
          "exceeds data length received. dataPlusChecksumLen=" +
          dataPlusChecksumLen + " header: " + header); 
    } 
    in.skipBytes(checksumLen);

    long startTime = now();
    IOUtils.readFully(in, buffers[icb].array(), 0, header.getDataLen());
    this.packetRecvTime += now() - startTime;
  }
  */
  
  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
  @SuppressWarnings("unused")
  int receivePacket() throws IOException {
    long startTime = now();
    long tsBase = 0l, tsRecvd=0l, tsWritten=0l, iar=0l, ibr=01, t1=0l, t2=0l, t3=0l;
    
    if (PerformanceTraceSwitch.getDataNodeTimeBreakDown() || PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite())
      tsBase = System.nanoTime();
    packetReceiver.receiveNextPacket(in);
    header = packetReceiver.getHeader();
    buffers[icb] = packetReceiver.getDataSlice();
    this.packetRecvTime += now() - startTime;
    if (PerformanceTraceSwitch.getDataNodeTimeBreakDown() || PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite())
      tsRecvd = System.nanoTime();
    LOG.info("Receive Packet at Time " + startTime);
    if (LOG.isDebugEnabled()) {
      LOG.debug("MemBlockReceiver: Receiving one packet for block " + block + ":" + header);
    }

    // Sanity check the header
    /*
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + " from " + inAddr + " at offset " +
                            header.getOffsetInBlock() + ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    */

    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + ") from " + inAddr + " at offset " +
                            header.getOffsetInBlock() + ": " + header.getDataLen()); 
    }

    long offsetInBlock = header.getOffsetInBlock();
    long seqno = header.getSeqno();
    boolean lastPacketInBlock = header.isLastPacketInBlock();
    int len = header.getDataLen();
    boolean syncBlock = header.getSyncBlock();

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
    }

    HybridLogicalClock mhlc = new HybridLogicalClock(header.getMhlc());

    LOG.info("Message HLC: " + mhlc);
    // update received bytes
    /*
    long firstByteInBlock = offsetInBlock;
    
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock);
    }
    */

    // put in queue for pending acks, unless sync was requested
    /*
    if (responder != null && !syncBlock) {
        ((PacketResponder) responder.getRunnable()).enqueue(seqno, lastPacketInBlock, offsetInBlock, Status.SUCCESS,
                                                            mhlc);
    }
    */

    // First write the packet to the mirror:
    if (mirrorOut != null && !mirrorError) {
      try {
        packetReceiver.mirrorPacketTo(mirrorOut);
        mirrorOut.flush();
      } catch (IOException e) {
        super.handleMirrorOutError(e);
      }
    }

    if (lastPacketInBlock || len == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Receiving an empty packet or the end of the block " + block);
      }
      // sync block if requested
      if (syncBlock) {
        flushOrSync(true);
      }
    } else {
      icb = 1-icb; // shift
      if (PerformanceTraceSwitch.getDataNodeTimeBreakDown() || 
          PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite())
        ibr = System.nanoTime();
      blockWriter.requestWrite(1-icb, mhlc, offsetInBlock, len);
      blockWriter.waitWrite();
      if (PerformanceTraceSwitch.getDataNodeTimeBreakDown() || 
          PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite())
        iar = System.nanoTime();
      datanode.metrics.incrBytesWritten(len);
    }

    // put in queue for pending acks, unless sync was requested.
    if (responder != null) {
      ((PacketResponder) responder.getRunnable()).enqueue(seqno, lastPacketInBlock, offsetInBlock, Status.SUCCESS,
                                                          mhlc);
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }

    if (PerformanceTraceSwitch.getDataNodeTimeBreakDown() || 
        PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite())
      tsWritten = System.nanoTime();

    if (PerformanceTraceSwitch.getDataNodetimeBreakDownNoWrite()) {
      System.out.println((tsRecvd - tsBase) + " " + (tsWritten-tsRecvd+ibr-iar) + " " + len);
      System.out.flush();
    }
    if (PerformanceTraceSwitch.getDataNodeTimeBreakDown()) {
      System.out.println((tsRecvd - tsBase) + " " + (tsWritten-tsRecvd) + " " + len);
      System.out.flush();
    }
    LOG.info("Last Packet in Block: " + lastPacketInBlock + ", Len: " + len);
    return lastPacketInBlock?-1:len;
  }

  void receiveBlock(
    DataOutputStream mirrOut,   // output to next datanode
    DataInputStream mirrIn,     // input from next datanode
    DataOutputStream replyOut,  // output to previous datanode
    String mirrAddr, DataTransferThrottler throttlerArg,
    DatanodeInfo[] downstreams) throws IOException {

    syncOnClose = datanode.getDnConf().syncOnClose;
    boolean responderClosed = false;
    mirrorOut = mirrOut;
    mirrorAddr = mirrAddr;
    throttler = throttlerArg;

    LOG.info("MemBlockReceiver::receiveBlock: Mirror Address := " + mirrAddr);
    try {
      if (isClient && !isTransfer) {
        responder = new Daemon(datanode.threadGroup, new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }

      // start blockWriter
      blockWriter = new BlockWriter((HLCOutputStream) this.out, this.buffers, this.replicaInfo, this.rp);

      this.packetRecvTime = 0;
      while (receivePacket() >= 0) { /* Receive until the last packet */ }
      LOG.info("CQDEBUG: MemBlockReceiver: packet receive time:" + this.packetRecvTime + "ms");

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) {
        // close the block/crc files
        close();
        block.setNumBytes(replicaInfo.getNumBytes());
        if (stage == BlockConstructionStage.TRANSFER_RBW) {
          // for TRANSFER_RBW, convert temporary to RBW
          datanode.data.convertTemporaryToRbw(block);
        } else {
          // for isDatnode or TRANSFER_FINALIZED
          // Finalize the block.
          datanode.data.finalizeBlock(block);
        }
        datanode.metrics.incrBlocksWritten();
      }
    } catch (IOException ioe) {
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      if (blockWriter != null) {
        try {
          LOG.info("Shutdown Writer\n");
          blockWriter.shutdown();
          LOG.info("Writer closed\n");
        } catch(Exception e) {
          //do nothing
        }
      }
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) {
            try {
              ((PacketResponder) responder.getRunnable()).
                  sendOOBResponse(PipelineAck.getRestartOOBStatus());
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure 
              // first. Insert a delay to lower the chance of client 
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            } catch (IOException ioe) {
              LOG.info("Error sending OOB Ack.", ioe);
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new IOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }
  
  String getStorageUuid() {
    return replicaInfo.getStorageUuid();
  }
  
  class PacketResponder implements Runnable, Closeable {   
    /** queue for packets waiting for ack - synchronization using monitor lock */
    private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
    /** the thread that spawns this responder */
    private final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    private volatile boolean running = true;
    /** input from the next downstream datanode */
    private final DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    private final DataOutputStream upstreamOut;
    /** The type of this responder */
    private final PacketResponderType type;
    /** for log and error messages */
    private final String myString; 
    private boolean sending = false;

    PacketResponder(final DataOutputStream upstreamOut, final DataInputStream downstreamIn,
                    final DatanodeInfo[] downstreams) {
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;
      this.type = downstreams == null? PacketResponderType.NON_PIPELINE
                : downstreams.length == 0? PacketResponderType.LAST_IN_PIPELINE
                : PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;

      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type != PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
      LOG.info("MemBlockReceiver: new PacketResponder " + this.myString);
    }

    @Override
    public String toString() {
      return myString;
    }

    private boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }

    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno
     * @param lastPacketInBlock
     * @param offsetInBlock
     */
    void enqueue(final long seqno, final boolean lastPacketInBlock, final long offsetInBlock, final Status ackStatus,
                 HybridLogicalClock mhlc) {
      final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock, System.nanoTime(), ackStatus, mhlc);

      if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ": enqueue " + p);
      }
      synchronized(ackQueue) {
        if (running) {
          ackQueue.addLast(p);
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus + ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(PipelineAck.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: " + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            ackStatus, null);
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }
    
    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() == 0) {
          // TODO: Remove.
          LOG.info(myString + ": seqno=" + seqno + " waiting for local datanode to finish write.");
          if (LOG.isDebugEnabled()) {
            LOG.debug(myString + ": seqno=" + seqno + " waiting for local datanode to finish write.");
          }
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.getFirst() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() != 0) {
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug(myString + ": closing");
        }
        running = false;
        ackQueue.notifyAll();
      }

      synchronized(this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      boolean lastPacketInBlock = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;

      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;

        try {
          Packet pkt = null;
          long expected = -2;
          PipelineAck ack = new PipelineAck();
          long seqno = PipelineAck.UNKOWN_SEQNO;
          long ackRecvNanoTime = 0;

          try {
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              // Read an ack from downstream datanode.
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK.
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) {
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    Status.SUCCESS, ack.getMhlc());
                continue;
              }
              seqno = ack.getSeqno();
            }
            if (seqno != PipelineAck.UNKOWN_SEQNO || type == PacketResponderType.LAST_IN_PIPELINE) {
              pkt = waitForAckHead(seqno);
              if (!isRunning()) {
                break;
              }
              expected = pkt.seqno;
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE && seqno != expected) {
                throw new IOException(myString + "seqno: expected=" + expected + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
                // The total ack time includes the ack times of downstream nodes. The value is 0 if this responder
                // doesn't have a downstream DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true;
          } catch (IOException ioe) {
            if (Thread.interrupted()) {
              isInterrupted = true;
            } else {
              // Continue to run even if can not read from mirror notify client of the error and wait for the client to
              // shut down the pipeline.
              mirrorError = true;
              LOG.info(myString, ioe);
            }
          }

          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check any other status updates from the receiver
             * thread (e.g. if it is ok to write to replyOut). It is prudent to not send any more status back to the
             * client because this datanode has a problem. The upstream datanode will detect that this datanode is bad,
             * and rightly so.
             *
             * The receiver thread can also interrupt this thread for sending an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false;
            continue;
          }

          if (lastPacketInBlock) {
            // Finalize the block and close the block file.
            finalizeBlock(startTime);
          }
          // TODO: Remove
          LOG.info("PacketResponder.run(): sendAckUpstream called with pkt " + pkt);
          sendAckUpstream(ack, expected, totalAckTimeNanos, (pkt != null ? pkt.offsetInBlock : 0),
                          (pkt != null ? pkt.ackStatus : Status.SUCCESS), (pkt != null ? pkt.mhlc: null));
          if (pkt != null) {
            // remove the packet from the ack queue
            removeAckHead();
          }
        } catch (IOException e) {
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            try {
              datanode.checkDiskError(e); // may throw an exception here
            } catch (IOException ioe) {
              LOG.warn("DataNode.checkDiskError failed in run() with: ", ioe);
            }
            LOG.info(myString, e);
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt();
            }
          }
        } catch (Throwable e) {
          LOG.warn("Exception in BlockReceiver.run(): ", e);
          if (running) {
            LOG.info(myString, e);
            running = false;
            receiverThread.interrupt();
          }
        }
      }

      LOG.info(myString + " terminating");
    }
    
    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     */
    private void finalizeBlock(long startTime) throws IOException {
      if (blockWriter != null) {
        try {
          blockWriter.shutdown();
          blockWriter = null;
        } catch(InterruptedException ie) {
          //do nothing
        }
      }
      // MemBlockReceiver.this.close();
      final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime()
          : 0;
      block.setNumBytes(replicaInfo.getNumBytes());
      datanode.data.finalizeBlock(block);
      datanode.closeBlock(
          block, DataNode.EMPTY_DEL_HINT, replicaInfo.getStorageUuid());
      if (ClientTraceLog.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block.getBlockPoolId());
        ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr, myAddr, block.getNumBytes(), "HDFS_WRITE",
                           clientname, offset, dnR.getDatanodeUuid(), block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " size " + block.getNumBytes() + " from " + inAddr);
      }
    }
    
    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     * @param mvc the message vector clock
     */
    private void sendAckUpstream(PipelineAck ack, long seqno, long totalAckTimeNanos, long offsetInBlock,
                                 Status myStatus, HybridLogicalClock mvc) throws IOException {
      // TODO: Remove msg.
      LOG.info("MemBlockReceiver: sendAckUpstream was called with the folowing arguments:");
      LOG.info("Seq No: " + seqno);
      LOG.info("Total Ack Time Nanos: " + totalAckTimeNanos);
      LOG.info("Offset In Block: " + offsetInBlock);
      LOG.info("Status: " + myStatus);
      LOG.info("TS: " + mvc);

      try {
        // Wait for other sender to finish. Unless there is an OOB being sent, the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait();
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos, offsetInBlock, myStatus, mvc);
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.  
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myStatus the local ack status
     * @param mvc message vector clock
     */
    private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno, long totalAckTimeNanos, long offsetInBlock,
                                            Status myStatus, HybridLogicalClock mhlc) throws IOException {
      Status[] replies = null;

      if (ack == null) {
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new Status[1];
        replies[0] = myStatus;
      } else if (mirrorError) { // ack read error
        replies = MIRROR_ERROR_STATUS;
      } else {
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new Status[1 + ackLen];
        replies[0] = myStatus;
        for (int i = 0; i < ackLen; i++) {
          replies[i + 1] = ack.getReply(i);
        }
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the 
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk.
        if (ackLen > 0 && replies[1] == Status.ERROR_CHECKSUM) {
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      }
      PipelineAck replyAck = new PipelineAck(seqno, replies, totalAckTimeNanos, mhlc/*HDFSRS_HLC*/);
 
      /*
      if (replyAck.isSuccess() && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      	// we don't need this.
      }*/

      // send my ack back to upstream datanode
      // TODO: Remove later.
      LOG.info("sendAckUpstreamUnprotected: seqno = " + seqno + ", time = " + System.currentTimeMillis());
      replyAck.write(upstreamOut);
      upstreamOut.flush();
      if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back. 
      if (myStatus == Status.ERROR_CHECKSUM) {
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }
    
    /**
     * Remove a packet from the head of the ack queue
     * 
     * This should be called only when the ack queue is not empty
     */
    private void removeAckHead() {
      synchronized(ackQueue) {
        ackQueue.removeFirst();
        ackQueue.notifyAll();
      }
    }
  }
  
  class BlockWriter implements Runnable {
    HLCOutputStream hlcout;
    ByteBuffer[] bufs;
    int curBufIdx=-1;
    HybridLogicalClock hlc;
    long offsetInBlock;
    int len;
    boolean isRunning;
    Thread thread;
    MemDatasetManager.MemBlockMeta replica;
    IRecordParser rp;
    
    public BlockWriter(HLCOutputStream vcout, ByteBuffer[] bufs, MemDatasetManager.MemBlockMeta replicaInfo, IRecordParser rp) {
      this.hlcout = vcout;
      this.bufs = bufs;
      this.replica = replicaInfo;
      this.rp = rp;
      isRunning = true;
      thread = new Thread(this);
      thread.start();
    }
    
    public void requestWrite(int icb, HybridLogicalClock hlc, long offsetInBlock ,int len) {
      synchronized(bufs) {
        while (this.curBufIdx != -1) {
          try {
            bufs.wait();
          } catch(InterruptedException e) {
            LOG.error("BlockWriter Error:" + e);
          }
        }
        this.curBufIdx = icb;
        this.hlc = hlc;
        this.offsetInBlock = offsetInBlock;
        this.len = len;
        bufs.notify();
      }
    }

    public void waitWrite() {
      synchronized(bufs) {
        while (this.curBufIdx != -1) {
          try {
            bufs.wait();
          } catch (InterruptedException e) {
            LOG.error("BlockWriter Error:" + e);
          }
        }
      }
    }

    public void shutdown() throws InterruptedException {
      synchronized(bufs) {
        isRunning = false;
        bufs.notify();
      }
      join();
    }
    
    private void join() throws InterruptedException {
      if (this.thread != null)
        this.thread.join();
    }
    
    private void sanityCheck() throws IOException {
      if (offsetInBlock > replica.getNumBytes())
        throw new IOException("Received an out-of-sequence packet for " + block + "from " + inAddr + " at offset " +
                              offsetInBlock + ". Expecting packet starting at " + replica.getNumBytes());
    }
    
    @Override
    public void run() {
      synchronized(bufs) {
        do {
          try {
            if (curBufIdx == 0 || curBufIdx == 1) {
              sanityCheck();
              // We assume that
              // 1) each buffer has one or multiple records.
              // 2) records are not splitted across buffer boundary.
              int sRec = 0;     // Start of the record.
              int recLen;       // Length of the record.
              ByteBuffer buf = bufs[curBufIdx];

              while (sRec != len) {
                recLen = rp.ParseRecord(buf);
                hlcout.write(hlc, rp.getUserTimestamp(), offsetInBlock, buf.array(), buf.arrayOffset() + sRec, recLen);
                sRec += recLen;
              }
              if (replica.getNumBytes() < replica.getBytesOnDisk())
                replica.setNumBytes(replica.getBytesOnDisk());
              curBufIdx = -1;
              bufs.notify();
            } else {
              bufs.wait();
            }
          } catch(InterruptedException ie) {
            // do nothing
          } catch(IOException ioe) {
            LOG.error("BlockWriter Error:" + ioe);
          } catch(RecordParserException rpe) {
            LOG.error("BlockWriter Error:" + rpe);
          }
        } while(isRunning || curBufIdx != -1);
      }
    }
  }
}
