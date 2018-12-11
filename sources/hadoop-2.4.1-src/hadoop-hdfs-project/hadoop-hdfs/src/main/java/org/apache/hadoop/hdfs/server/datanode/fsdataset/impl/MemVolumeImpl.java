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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;


/**
 * The underlying volume used to store replica.
 * 
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
class MemVolumeImpl implements FsVolumeSpi {
  private final MemDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final long usage;           
  private final long reserved;
  private HashMap<String, Long> bpSlices;
  
  MemVolumeImpl(MemDatasetImpl dataset, String storageID, File currentDir, Configuration conf, 
      StorageType storageType) throws IOException {
    this.dataset = dataset;
    this.storageID = storageID;
    this.reserved = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT);
    // this.usage = dataset.memManager.getCapacity();
    File parent = currentDir.getParentFile();
    this.usage = (new DF(parent, conf)).getCapacity();
    this.storageType = storageType;
    this.bpSlices = new HashMap<String, Long>();
  }
  
  void decDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      Long bp = bpSlices.get(bpid);
      if (bp != null) {
        bpSlices.put(bpid, bp - value);
      }
    }
  }
  
  void incDfsUsed(String bpid, long value) {
    synchronized(dataset) {
      Long bp = bpSlices.get(bpid);
      if (bp != null) {
        bpSlices.put(bpid, bp + value);
      }
    }
  }
  
  long getDfsUsed() throws IOException {
    long dfsUsed = 0;
    synchronized(dataset) {
      for(Long s : bpSlices.values()) {
        dfsUsed += s;
      }
    }
    return dfsUsed;
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    return bpSlices.get(bpid);
  }
  
  /**
   * Calculate the capacity of the filesystem, after removing any
   * reserved capacity.
   * @return the unreserved number of bytes left in this filesystem. May be zero.
   */
  long getCapacity() {
    long remaining = usage - reserved;
    return remaining > 0 ? remaining : 0;
  }

  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity()-getDfsUsed();
    return (remaining > 0) ? remaining : 0;
  }
    
  long getReserved(){
    return reserved;
  }

  @Override
  public String getBasePath() {
    return "mem";
  }
  
  @Override
  public String getPath(String bpid) throws IOException {
    return "mem/" + bpid;
  }
  
  @Override
  public String toString() {
    return "mem";
  }
  
  @Override
  public File getFinalizedDir(String bpid) throws IOException {
    return null;
  }

  /**
   * Make a deep copy of the list of currently active BPIDs
   */
  @Override
  public String[] getBlockPoolList() {
    return bpSlices.keySet().toArray(new String[bpSlices.keySet().size()]);   
  }

  void shutdown() {
    bpSlices.clear();
  }

  void addBlockPool(String bpid, Configuration conf) throws IOException {
    if (!bpSlices.containsKey(bpid))
      bpSlices.put(bpid, 0L);
  }
  
  void shutdownBlockPool(String bpid) {
    bpSlices.remove(bpid);
  }

  @Override
  public String getStorageID() {
    return storageID;
  }
  
  @Override
  public StorageType getStorageType() {
    return storageType;
  }
  
  DatanodeStorage toDatanodeStorage() {
    return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, storageType);
  }

}

