#!/bin/bash
cd ../../java
javah -o JNIBlog.h edu.cornell.cs.blog.JNIBlog
mv JNIBlog.h $OLDPWD
cd $OLDPWD
