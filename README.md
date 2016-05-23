# The Freeze Frame File System

Many applications perform real-time analysis on data streams. We argue that existing solutions are poorly matched to the need, and introduce our new Freeze-Frame File System. Freeze-Frame FS is able to accept streams of updates while satisfying “temporal reads” on demand. The system is fast and accurate: we keep all update history in a memory-mapped log, cache recently retrieved data for repeat reads, and use a hybrid of a real-time and a logical clock to respond to read requests in a manner that is both temporally precise and causally consistent. When RDMA hardware is available, the write and read throughput of a single client reaches 2.6G Byte/s for writes, 5G Byte/s for reads, close to the limit on the hardware used in our experiments. Even without RDMA, Freeze Frame FS substantially outperforms existing file system options for our target settings.

We are reorganizing the source code and moving the project from [Codeplex](http://fffs.codeplex.com) to here...