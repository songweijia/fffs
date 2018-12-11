package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

/**
 * A factory for creating {@link MemDatasetImpl} objects.
 */
public class MemDatasetFactory extends FsDatasetSpi.Factory<MemDatasetImpl> {
  @Override
  public MemDatasetImpl newInstance(DataNode datanode,
      DataStorage storage, Configuration conf) throws IOException {
    return new MemDatasetImpl(datanode, storage, conf);
  }
}