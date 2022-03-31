package com.deerlili.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	//创建filesystem对象，该对象拥有对hdfs的操作的方法
	public static FileSystem getFileSystem() throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		return hdfs;
	}
	
	//创建一个文件夹
	public static void mkDir(FileSystem hdfs,String dirName) throws Exception {
		Path path = new Path(dirName);
		if(hdfs.mkdirs(path)) {
			System.out.println("创建目录："+dirName+" 成功");
		}else {
			System.out.println("创建目录："+dirName+" 失败");
		}
	}
	
	public static void main(String[] args) throws Exception {
		FileSystem hdfs = getFileSystem();
		mkDir(hdfs, "/dirFromJava");
	}
}
