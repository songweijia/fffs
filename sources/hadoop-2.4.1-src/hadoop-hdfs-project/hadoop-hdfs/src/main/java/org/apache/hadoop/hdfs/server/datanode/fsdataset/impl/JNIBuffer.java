package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.nio.ByteBuffer;

public class JNIBuffer {
  static {
    System.loadLibrary("org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer");
  }
  
  public native ByteBuffer createBuffer(int capacity);
  public native ByteBuffer getBuffer(int bufID, int offset, int limit);
  public native void deleteBuffers();
  public native void printBuffer();

  public static void main(String[] args) {
    JNIBuffer hjni = new JNIBuffer();
    ByteBuffer b1 = hjni.createBuffer(100);
    hjni.printBuffer();
    b1.put("Overwriting the buffer".getBytes());
    hjni.printBuffer();

    ByteBuffer b2 = hjni.createBuffer(100);
    hjni.printBuffer();
    hjni.deleteBuffers();
  }
}