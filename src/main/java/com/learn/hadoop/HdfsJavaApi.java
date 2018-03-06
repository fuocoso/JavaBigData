package com.learn.hadoop;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsJavaApi {
	//1.获取HDFS java对象
	FileSystem fs = null;
	
	@Before
	public void getFS() throws URISyntaxException, IOException, InterruptedException {
		//指定NameNode的访问入口
		URI uri = new URI("hdfs://com.apache.bigdata:8020");
		
		//获取Hadoop HDFS的配置文件 {HADOOP_HOME}/etc/hadoop/core-site.xml hdfs-site.xml
		Configuration conf = new Configuration();
		
		
		//设置访问HDFS的用户名
		String user = "bigdata";
		
		//2.获取具体的HDFS对象
		fs = FileSystem.get(uri, conf, user);
	}
	
	/**
	 * 从HDFS 下载文件到本地（windows）
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException {
		/**
		 * FSDataInputStream 使用的是NIO
		 */
		//通过FileSystem构造输入流
		FSDataInputStream in = fs.open(new Path("/input/1.log"));
		
		//构建输出流将读到内存的hdfs文件保存到本地文件系统 -> 具体的文件不是目录
		FileOutputStream  out = new FileOutputStream("D:\\demo\\1.log");
		
		IOUtils.copyBytes(in, out, 4096, true);
		/**
		 * 参数说明
		 * in  输入流
		 * out 输出流
		 * 4096 缓冲区大小
		 * true  是否自动关闭输入输出流，设置为true自动关闭，false 需要自己关闭
		 * IOUtils.closeStream(in);
		 * IOUtils.closeStream(out);
		 */			
	}
	
	/**
	 * 测试上传文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		
		//通过FileSystem的cerate()构建输出流将文件保存到HDFS
		FSDataOutputStream out = fs.create(new Path("/input/xx.txt"), true);
		
		//构成输入流将要上传的文件读到内存中
		FileInputStream in = new FileInputStream("E:\\课堂\\170404\\Hadoop\\Day01-0721.txt");
 		
		IOUtils.copyBytes(in, out, 4096, true);
		
	}
	
	/**
	 * 在HDFS上创建目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {
		boolean issuccess = fs.mkdirs(new Path("/input/test/test1"));
		System.err.println(issuccess);		
	}
	
	/**
	 * 删除HDFS上的文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public  void  tsetRm() throws IllegalArgumentException, IOException {
		//boolean issuccess =  fs.deleteOnExit(new Path("/input/test"));
		boolean rm = fs.delete(new Path("/input/test"), true);
		System.out.println(rm);
	}
}
