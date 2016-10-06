# 1. Overview

Many applications perform real-time analysis on data streams. We argue that existing solutions are poorly matched to the need, and introduce our new Freeze-Frame File System. Freeze-Frame FS is able to accept streams of updates while satisfying “temporal reads” on demand. The system is fast and accurate: we keep all update history in a memory-mapped log, cache recently retrieved data for repeat reads, and use a hybrid of a real-time and a logical clock to respond to read requests in a manner that is both temporally precise and causally consistent. When RDMA hardware is available, the write and read throughput of a single client reaches 2.6G Byte/s for writes, 5G Byte/s for reads, close to the limit on the hardware used in our experiments. Even without RDMA, Freeze Frame FS substantially outperforms existing file system options for our target settings.

# 2. Build Instructions

To build FFFS, please download [hadoop 2.4.1 source code](https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz); apply it with the patch in `sources/fffs-for-hadoop-2.4.1-src.patch.tgz`. Then, build the patched source code.

## 2.1 System Requirement

Although we only tried CentOS 6.5 and Ubuntu 16.04/14.04/12.10, FFFS can be built by any recent linux distribution. Please make sure you have at least 10G diskspaces and have the following software installed:
* gcc, g++, and make
* cmake >= version 2.8.0
* openssl development package (CentOS: sudo yum install openssl openssl-devel; Ubuntu: sudo apt-get install openssl libssl-dev)
* zlib development package (CentOS: sudo yum install zlib zlib-devel; Ubuntu: sudo apt-get install zlib1g zlib1g-dev)
* protobuf-2.5.0
* Oracle Java SE 7
* Apache Ant 1.9.6
* Apache Maven 3.1.1
* [OFED](http://downloads.openfabrics.org/OFED/) >= 1.5

## 2.2 Download Source and Apply the Patch
Download [hadoop-2.4.1 tarball](https://archive.apache.org/dist/hadoop/core/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz). Unpack it. Download [FFFS patch](https://github.com/songweijia/fffs/blob/master/sources/fffs-for-hadoop-2.4.1-src.patch.tgz), unpack it and put it in the extracted folder hadoop-2.4.1-src. Patch the source code as follows:

` -p1 < fffs-for-hadoop-2.4.1-src.patch`

## 2.3 Build FFFS

Make sure current path is hadoop-2.4.1-src. Use the following command to build FFFS:

`> mvn package -Pnative,dist -Dtar -DskipTests`

This will take a while. After it finishes successfully, find the binary package at hadoop-2.4.1-src/hadoop-dist/target/hadoop-2.4.1.tar.gz. Use this package for deployment.

# 3 Deployment

Deploying FFFS is basically the same as deploying the original HDFS. Please follow the online hadoop guide for how to deploy HDFS. Note that we need a working HDFS setup from this point to continue. We assume the users are familiar with HDFS deployment. To enable FFFS, set the following configurations in /etc/hadoop/hdfs-site.xml:

1) Enable the FFFS block log, and set the memory buffer size for it.
```xml
<property>
  <name>dfs.datanode.fsdataset.factory</name>
  <value>org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MemDatasetFactory</value>
</property>
<property>
  <name>dfs.memory.capacity</name>
  <value>34359738368</value><!--32GB -->
</property>
```
2) PageSize: the default size is 4KB. Small page size relieve internal fragmentation but cause higher overhead. If the workload mainly consists of very small writes, use page smaller than 4KB.
```xml
<property>
  <name>dfs.memblock.pagesize</name>
  <value>4096</value>
</property>
```
3) Packet size represents the maximum write log resolution. The default packet size is 64KB. Use larger one for better performance if the application is tolerant to coarse log resolution.
```xml
<property>
  <name>dfs.client-write-packet-size</name>
  <value>65536</value>
</property>
```
4) Turn off checksum.
FFFS relies on TCP checksum instead of using another layer of checksum. We plan to support stronger data integrity check in future work.
``` xml
<property>
  <name>dfs.checksum.type</name>
  <value>NULL</value>
</property>
```
5) Turn off replication.
FFFS does not support block replication but we plan to support on demand caching to enable high performance with many readers, which is faster and more space-efficient.
``` xml
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
```
6) Optional: RDMA settings

Set the name of the rdma device.
```xml
<property>
  <name>dfs.rdma.device</name>
  <value>mlx5_0</value>
</property>
```
Set the size of client side memory buffer:
```xml
<property>
  <name>dfs.rdma.client.mem.region.size.exp</name>
  <value>30</value> <!--2^30=1GB-->
</property>
```
Enable RDMA for read and write:
```xml
<property>
  <name>dfs.client.use.rdma.blockreader</name>
  <value>true</value>
</property>
<property>
  <name>dfs.client.use.rdma.blockwriter</name>
  <value>true</value>
</property>
```
Set the RDMA transfer size:
```xml
<property>
  <name>dfs.client.rdma.writer.flushsize</name>
  <value>2097152</value> <!--2MB-->
</property>
```
# 4 Usage
FFFS APIs are compatible with HDFS API. FFFS reuses the HDFS snapshot interface but has a totally difference implemntation under the hood. HDFS treats all updates from when a file is opened until when it is closed as a single atomic event that occurred when the file was opened. But the file data might not be finalized until the file is closed, possibly ten minutes later. In consequence, an HDFS snapshot created at 10:00 a.m. might include updates that didn’t occur until 10:09am. FFFS prevent this by a unique logical clock based solution. Please refer to HDFS document for how to create and read from snapshots.

Whereas HDFS only permits appends, FFFS allows updates at arbitrary offsets within files. To support this, we enable use of the seek system call HDFS applications does not need to be modified to use FFFS. The following codelets show how to write randomly to FFFS.
```java
void randomWrite(FileSystem fs, String path)
throws IOException{
  Path p = new Path(path);
  byte [] buf = new byte[4096];
  int i;
  // 0) Initialize write 4KB for 1024 times.
  for(i=0;i<4096;i++)buf[i]='I';
  FSDataOutputStream fsos = fs.create(new Path(path));
  for(i=0;i<1024;i++)fsos.write(buf,0,4096);
  // 1) write 4K at 0; 4K at 1044480; 4K at 100000
  for(i=0;i<4096;i++)buf[i]='1';
  fsos.seek(0);fsos.write(buf,0,4096);
  fsos.seek(1044480);fsos.write(buf,0,4096);
  fsos.seek(100000);fsos.write(buf,0,4096);
  // 2) write cross blocks, since we set block size to 1MB
  for(i=0;i<4096;i++)buf[i]='2';
  fsos.seek(1048000);fsos.write(buf,0,1000);
  fsos.seek(2097000);
  for(int j=0;j<1049;j++)fsos.write(buf,0,1000);
  fsos.close();
}
```
Please see more examples in [`sources/examples/FileTester.java`](https://github.com/songweijia/fffs/blob/master/sources/examples/FileTester.java)

This project with its previous versions is also maintained at [Codeplex](http://fffs.codeplex.com).
