#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include "org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer.h"

std::vector<char *> data_bufs;

JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer_createBuffer(
		JNIEnv *env, jobject thisObj, jint capacity) {
	char * data = (char *) malloc(capacity);
	data_bufs.push_back(data);

	jobject buf = env->NewDirectByteBuffer((void *) data, capacity);
	jobject ref = env->NewGlobalRef(buf);
	return ref;
}

JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer_getBuffer(JNIEnv *env,
		jobject thisObj, jint bufID, jint offset, jint limit) {
	char * data = data_bufs[bufID];

	jobject buf = env->NewDirectByteBuffer((void *)(data + offset), limit);
	jobject ref = env->NewGlobalRef(buf);
	return ref;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer_deleteBuffers(JNIEnv *env, jobject thisObj) {
	for (std::vector<char *>::iterator it = data_bufs.begin(); it != data_bufs.end(); it++) {
		free(*it);
	}
	data_bufs.clear();
	return;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_hdfs_server_datanode_fsdataset_impl_JNIBuffer_printBuffer(JNIEnv *env, jobject thisObj) {
	for (std::vector<char *>::iterator it = data_bufs.begin(); it != data_bufs.end(); it++) {
		printf("buffer:%s\n", *it);
	}
}
