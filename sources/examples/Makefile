PROG=FileTester.jar
JSRC=FileTester.java
HADOOP_HOME=/home/ubuntu/opt/hadoop
M2_REPO=/home/ubuntu/.m2/repository
JAR_FILE_PATH:=$(shell find $(M2_REPO) -name '*.jar')
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
JAR_FILE_LIST = $(subst $(SPACE),:,$(JAR_FILE_PATH))
CLASSPATH="${HADOOP_HOME}/share/hadoop/hdfs/hadoop-hdfs-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/hadoop-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.4.1.jar:${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-annotations-2.4.1.jar:${HADOOP_HOME}/share/hadoop/common/lib/hadoop-auth-2.4.1.jar:/home/ubuntu/.m2/repository/org/slf4j/slf4j-api/1.5.6/slf4j-api-1.5.6.jar:${JAR_FILE_LIST}:."

$(PROG): $(JSRC)
	javac -cp ${CLASSPATH} $(JSRC)
	jar cvfm $(PROG) Manifest.txt *.class
	java -cp ${CLASSPATH} FileTester

clean:
	rm $(PROG) *.class
