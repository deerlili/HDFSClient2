package com.deerlili.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class HdfsUtils {
	
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
	
	//在hdfs上创建一个新的文件，将数据写入hdfs中
	public static void createFile(String fileName,String content) throws Exception {
		Path path = new Path(fileName);
		if(hdfs.exists(path)) {
			System.out.println("文件: "+fileName+" 在hdfs上已经存在");
		}else {
			FSDataOutputStream outputStream = hdfs.create(path);
			outputStream.writeUTF(content);
			outputStream.flush();
			outputStream.close();
		}
	}
	
	//读取hdfs上已有的文件
	public static void readFile(String fileName) throws Exception {
		Path path = new Path(fileName);
		if(!hdfs.exists(path) || hdfs.isDirectory(path)) {
			System.out.println("给定的路径："+fileName+"不存在，或者不是一个文件");
		}else {
			FSDataInputStream inputStream = hdfs.open(path);
			String content = inputStream.readUTF();
			System.out.println(content);
		}
	}
	
	//删除hdfs上已有的文件或文件夹
	public static void deleteFile(String fileName) throws Exception {
		Path path = new Path(fileName);
		if(!hdfs.exists(path)) {
			System.out.println("给定的路径："+fileName+"不存在");
		}else {
			hdfs.delete(path, true);
		}
	}
	
	//往hdfs上传文件
	public static void upload(String srcPath,String dstPath) throws Exception {
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		hdfs.copyFromLocalFile(src, dst);
	}
	
	//从hdfs下载文件
	public static void download(String srcPath,String dstPath) throws Exception {
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		hdfs.copyToLocalFile(src, dst);
	}
	
	//在hdfs上面对文件重命名
	public static void rename(String srcPath,String dstPath) throws Exception {
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		hdfs.rename(src, dst);
	}
	
	//现在hdfs上所有的文件
	public static void list(String srcPath) throws Exception {
		Path path = new Path(srcPath);
		RemoteIterator<LocatedFileStatus> listFiles = hdfs.listFiles(path, true);
		while(listFiles.hasNext()){
			LocatedFileStatus lfs = listFiles.next();
			Path path2 = lfs.getPath();
			System.out.println("上传时间："+lfs.getAccessTime()+" 文件名：" + 
			path2.getName()+"上传人：" +lfs.getOwner()+" 分块信息：" + Arrays.toString(lfs.getBlockLocations()));
		}
		
		System.out.println("--------------");
		FileStatus[] fileStatus = hdfs.listStatus(path);
		for (FileStatus fs : fileStatus) {
			System.out.println(fs);
		}
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
	
	public static void upload2(String hdfsPath,String localPath) throws Exception {
		Path dst = new Path(hdfsPath);
		File src = new File(localPath);
		FSDataOutputStream fos = hdfs.create(dst);
		FileInputStream fis = new FileInputStream(src);
		/*byte[] arr = new byte[1024*8];
		int len;
		while((len = fis.read(arr)) != -1) {								
			fos.write(arr,0,len);											
		}
		fis.close();
		fos.close();*/
//		IOUtils.copy(fis, fos);
	}
	
	public static void main(String[] args) throws Exception {
		String content = "yzsf132113sss";
		String fileName = "/1.txt";
//		createFile(fileName, content);
//		readFile(fileName);
//		deleteFile(fileName);
//		upload("d:/a/1.jpg", "/2.jpg");
//		download("/2.jpg", "d:/a/2.jpg");
//		rename("/123", "/bd14");
//		list("/");
//		getAllFileStatus("/");
		upload2("/1.txt", "d:/a/test.txt");
	}
	
}
