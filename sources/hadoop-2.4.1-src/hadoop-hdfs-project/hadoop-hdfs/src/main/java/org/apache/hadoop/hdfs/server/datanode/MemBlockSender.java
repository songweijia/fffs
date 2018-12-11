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

import java.io.DataOutputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetManager.MemBlockMeta;

import edu.cornell.cs.sa.HybridLogicalClock;


/**
 * Reads a block from the disk and sends it to a recipient.
 * 
 * Data sent from the BlockeSender in the following format:
 * <br><b>Data format:</b> <pre>
 *    +--------------------------------------------------+
 *    | ChecksumHeader | Sequence of data PACKETS...     |
 *    +--------------------------------------------------+ 
 * </pre>   
 * <b>ChecksumHeader format:</b> <pre>
 *    +--------------------------------------------------+
 *    | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
 *    +--------------------------------------------------+ 
 * </pre>   
 * An empty packet is sent to mark the end of block and read completion.
 * 
 * PACKET Contains a packet header, checksum and data. Amount of data
 * carried is set by BUFFER_SIZE.
 * <pre>
 *   +-----------------------------------------------------+
 *   | Variable length header. See {@link PacketHeader}    |
 *   +-----------------------------------------------------+
 *   | x byte checksum data. x is defined below            |
 *   +-----------------------------------------------------+
 *   | actual data ......                                  |
 *   +-----------------------------------------------------+
 * 
 *   Data is made of Chunks. Each chunk is of length <= BYTES_PER_CHECKSUM.
 *   A checksum is calculated for each chunk.
 *  
 *   x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
 *       CHECKSUM_SIZE
 *  
 *   CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32) 
 *  </pre>
 *  
 *  The client reads data until it receives a packet with 
 *  "LastPacketInBlock" set to true or with a zero length. If there is 
 *  no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK.
 */
class MemBlockSender extends BlockSender {
  /**
   * Constructor
   * 
   * @param block Block that is being read
   * @param startOffset starting offset to read from
   * @param length length of data to read
   * @param datanode datanode from which the block is being read
   * @param clientTraceFmt format string used to print client trace logs
   * @throws IOException
   */
  MemBlockSender(ExtendedBlock block, long startOffset, long length, DataNode datanode, String clientTraceFmt,
                 HybridLogicalClock myHLC, long timestamp, boolean bUserTimestamp) throws IOException {
    super(block, datanode, clientTraceFmt, myHLC);
    try {
      final MemBlockMeta replica;
      final long replicaVisibleLength;
      synchronized(datanode.data) { 
        replica = (MemBlockMeta)getReplica(block, datanode);
        replicaVisibleLength = replica.getVisibleLength();
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      }

      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }

      // transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
      // use normal transfer in those cases
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
        (!is32Bit || length <= Integer.MAX_VALUE);
 
      length = length < 0 ? replicaVisibleLength : length;

      // end is either last byte on memory
      long end = replica.getBytesOnDisk(timestamp, bUserTimestamp);
      if (startOffset < 0 || startOffset > end
          || (length + startOffset) > end) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + end + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
            ":sendBlock() : " + msg);
        throw new IOException(msg);
      }
      
      // Ensure read offset is position at the beginning of chunk
      offset = startOffset - (startOffset % chunkSize);
      if (length >= 0) {
        // Ensure endOffset points to end of chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % chunkSize != 0) {
          tmpLen += (chunkSize - tmpLen % chunkSize);
        }
        if (tmpLen < end) {
          // will use on-disk checksum here since the end is a stable chunk
          end = tmpLen;
        }
      }
      endOffset = end;
      seqno = 0;

      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("replica=" + replica);
      }
      blockIn = datanode.data.getBlockInputStream(block, offset, timestamp, bUserTimestamp); // seek to offset
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      IOUtils.closeStream(blockIn);
      throw ioe;
    }
  }

  /**
   * close opened streams.
   */
  @Override
  public void close() throws IOException {
    if(blockIn != null) {
      try {
        blockIn.close(); // close data file
      } catch (IOException e) {
        throw e;
      }
      blockIn = null;
    }
  }

  /**
   * Sends a packet with up to maxChunks chunks of data.
   * 
   * @param pkt buffer used for writing packet data
   * @param maxChunks maximum number of chunks to send
   * @param out stream to send data to
   * @param transferTo use transferTo to send data
   * @param throttler used for throttling data transfer bandwidth
   */
  private int sendPacket(ByteBuffer pkt, int maxChunks, OutputStream out,
      boolean transferTo, DataTransferThrottler throttler) throws IOException {
    int dataLen = (int) Math.min(endOffset - offset,
                             (chunkSize * (long) maxChunks));
    
    int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the packet
    int checksumDataLen = numChunks * checksumSize;
    int packetLen = dataLen + checksumDataLen + 4;
    
    // The packet buffer is organized as follows:
    // _______HHHHCCCCD?D?D?D?
    //        ^   ^
    //        |   \ checksumOff
    //        \ headerOff
    // _ padding, since the header is variable-length
    // H = header and length prefixes
    // C = checksums
    // D? = data, if transferTo is false.
    
    int headerLen = writePacketHeader(pkt, dataLen, packetLen, this.hlc/*HDFSRS_VC*/);
    
    // Per above, the header doesn't start at the beginning of the
    // buffer
    int headerOff = pkt.position() - headerLen;
    
    int checksumOff = pkt.position();
    byte[] buf = pkt.array();
    
    int dataOff = checksumOff + checksumDataLen;
    if (!transferTo) { // normal transfer
      IOUtils.readFully(blockIn, buf, dataOff, dataLen);
    }
    
    try {
      if (transferTo) {
        SocketOutputStream sockOut = (SocketOutputStream)out;
        // First write header and checksums
        sockOut.write(buf, headerOff, dataOff - headerOff);
        
        // no need to flush since we know out is not a buffered stream
        FileChannel fileCh = ((FileInputStream)blockIn).getChannel();
        LongWritable waitTime = new LongWritable();
        LongWritable transferTime = new LongWritable();
        sockOut.transferToFully(fileCh, blockInPosition, dataLen, 
            waitTime, transferTime);
        datanode.metrics.addSendDataPacketBlockedOnNetworkNanos(waitTime.get());
        datanode.metrics.addSendDataPacketTransferNanos(transferTime.get());
        blockInPosition += dataLen;
      } else {
        // normal transfer
        out.write(buf, headerOff, dataOff + dataLen - headerOff);
      }
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException) {
        /*
         * writing to client timed out.  This happens if the client reads
         * part of a block and then decides not to read the rest (but leaves
         * the socket open).
         */
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to send data:", e);
        } else {
          LOG.info("Failed to send data: " + e);
        }
      } else {
        /* Exception while writing to the client. Connection closure from
         * the other end is mostly the case and we do not care much about
         * it. But other things can go wrong, especially in transferTo(),
         * which we do not want to ignore.
         *
         * The message parsing below should not be considered as a good
         * coding example. NEVER do it to drive a program logic. NEVER.
         * It was done here because the NIO throws an IOException for EPIPE.
         */
        String ioem = e.getMessage();
        if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
          LOG.error("MemBlockSender.sendChunks() exception: ", e);
        }
      }
      throw ioeToSocketException(e);
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return dataLen;
  }
  
  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @return total bytes read, including checksum data.
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
    if (out == null) {
      throw new IOException( "out stream is null" );
    }
    initialOffset = offset;
    long totalRead = 0;
    OutputStream streamForSendChunks = out;

    final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
    try {
      int maxChunksPerPacket;
      int pktBufSize = PacketHeader.PKT_MAX_HEADER_LEN;
      boolean transferTo = transferToAllowed && baseStream instanceof SocketOutputStream
          && blockIn instanceof FileInputStream;
      if (transferTo) {
        FileChannel fileChannel = ((FileInputStream)blockIn).getChannel();
        blockInPosition = fileChannel.position();
        streamForSendChunks = baseStream;
        maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);
        
        // Smaller packet size to only hold checksum when doing transferTo
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1,
            numberOfChunks(HdfsConstants.IO_FILE_BUFFER_SIZE));
        // Packet size includes both checksum and data
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }

      ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

      while (endOffset > offset && !Thread.currentThread().isInterrupted()) {
        long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
            transferTo, throttler);
        offset += len;
        totalRead += len + (numberOfChunks(len) * checksumSize);
        seqno++;
      }
      // If this thread was interrupted, then it did not send the full block.
      if (!Thread.currentThread().isInterrupted()) {
        try {
          // send an empty packet to mark the end of the block
          sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo,
              throttler);
          out.flush();
        } catch (IOException e) { //socket error
          throw ioeToSocketException(e);
        }

        sentEntireByteRange = true;
      }
    } finally {
      if (clientTraceFmt != null) {
        final long endTime = System.nanoTime();
        ClientTraceLog.info(String.format(clientTraceFmt, totalRead,
            initialOffset, endTime - startTime));
      }
      close();
    }
    return totalRead;
  }
}
