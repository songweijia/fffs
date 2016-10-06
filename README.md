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

Previous versions are maintained at [Codeplex](http://fffs.codeplex.com).
