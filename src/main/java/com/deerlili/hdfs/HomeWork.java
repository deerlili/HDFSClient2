package com.deerlili.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HomeWork {
	public static final Configuration CONF = new Configuration();
	public static  FileSystem hdfs;
	
	static {
		try {
			hdfs = FileSystem.get(CONF);
		} catch (IOException e) {
			System.out.println("无法连接hdfs,请检查配置");
			e.printStackTrace();
		}
	}
	
	//利用流的形式往hdfs上传文件
	public static void upload(String hdfsPath,String localPath) throws Exception {
		Path dst = new Path(hdfsPath);
		File src = new File(localPath);
		FSDataOutputStream fos = hdfs.create(dst);
		FileInputStream fis = new FileInputStream(src);
		byte[] arr = new byte[1024*8];
		int len;
		while((len = fis.read(arr)) != -1) {								
			fos.write(arr,0,len);											
		}
		fis.close();
		fos.close();
//		IOUtils.copy(fis, fos);
	}
	
	//利用流的形式在hdfs上下载
	public static void download(String hdfsPath,String localPath) throws Exception {
		Path src = new Path(hdfsPath);
		File dst = new File(localPath);
		FSDataInputStream fis = hdfs.open(src);
		FileOutputStream fos = new FileOutputStream(dst);
		byte[] arr = new byte[1024*8];
		int len;
		while((len = fis.read(arr)) != -1) {								
			fos.write(arr,0,len);											
		}
		fis.close();
		fos.close();
//		IOUtils.copy(fis, fos);
	}
	
	public static void getAllFileStatus(String srcPath) throws Exception {
		Path path = new Path(srcPath);
		
		FileStatus[] fileStatus = hdfs.listStatus(path);
		
		for (FileStatus fs : fileStatus) {
			if(fs.isDirectory()) {
				getAllFileStatus(fs.getPath().toString());
			}else {
				System.out.println(fs);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
//		upload("/a.jpg", "d:/a/1.jpg");
		download("/a.jpg", "d:/a/2.jpg");
//		getAllFileStatus("/bd14");
	}
}
