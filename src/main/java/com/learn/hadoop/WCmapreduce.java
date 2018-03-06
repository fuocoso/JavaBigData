package com.learn.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WCmapreduce extends Configured implements Tool {
	/**
	 * LongWritable Text IntWritable这是都是HDFS的特有的数据类型，都实现了Hadoop的序列化接口Writable
	 * LongWritable Text IntWritable直接实现的WritableComparable，带有比较功能
	 * @author Administrator
	 * Hadoop  Java  HTML   Hadoop
	 */
	public static class  WCmapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		/**
		 * map阶段，数据一行一行的读进来的，作为map输入
		 * map映射  <偏移量，每一行的内容>  -》 <0,Hadoop  Java  HTML   Hadoop>
		 */
		private  Text mapOutkey = new Text();
		private  IntWritable  MapOutput =new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("< "+key+" -> "+value+" >");
			//1.将每一行的内容转成String
			String line = value.toString();
			
			//2.将String类型的每一行内容转成Sting[] str=[Hadoop,java,html,hadoop]
			//String[] str = line.split(" ");
			
			/**
			 * StringTokenizer分割字符串的类
			 * StringTokenizer(被切割内容, 
			 * " \t\n\r\f,.",默认为\b空格  \t \n \r换页  \f单词边界
			 * 是否将分隔符作为切分后的元素)
			 */
			
			StringTokenizer sT = new StringTokenizer(line, " \t\n\r\f,.");
			while (sT.hasMoreTokens()) {
				mapOutkey.set(sT.nextToken());
				context.write(mapOutkey, MapOutput);
			}
			
//			for (String word :str) {
//				//将每个单词转成Text 便于在hadoop节点之间进行数据传递				
//				mapOutkey.set(word);
//				//System.out.println("< "+mapOutkey+" -> "+MapOutput+" >");
//				//map输出的<key,vaule> -> <Hadoop,1>,<Java,1>,<html,1>,<Hadoop,1>
//				context.write(mapOutkey, MapOutput);
//			}						
		}		
	}
	
	/**
	 * Combiner编程
	 */
	public static class MyCombiner  extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
			int sum = 0;
			//System.out.print("<  "+arg0+ " -> ");
			//lsit[1,1,1,1....]
			for (IntWritable value: arg1) {
				sum += value.get();	
				//System.out.print(value.get()+",");
			}
			//System.out.print(" >"+"\n");
			//将每个单词及其出现的次数，输出到结果文件 part-00000
			System.out.print("< Combiner "+arg0+ " -> ");
			System.out.print(new IntWritable(sum)+ ">"+"\n");
			arg2.write(arg0, new IntWritable(sum));	
						
		}				
	}
	
	/**
	 * shuffle 过程
	 * 分区：mapredcue是分布式并行计算框架，采用分而治之的意思，理论上讲reducer越多，任务并行度越高，
	 * 意味着有更多的节点参与计算，速度会更快，总而言之，一般会有多个reducer，那么每个 reducer如何获取数据？
	 *  hadoop默认的方式是 对map输出的<key,value>中的key  key.hashcode() % N  N=设置的reducer个数
	 *  
	 *  分组：对每个分区内 <key,value>对 中，key相同的value放到一个分组内
	 *  
	 *  map输出的<key,vaule> -> <Hadoop,1>,<Java,1>,<html,1>,<Hadoop,1>
	 *  <Hadoop,list[1,1]>   
	 *  <Java,1>
	 *  <html,1>
	 */
	
	public static class MyPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String str = key.toString();
			int mapoutkey = Integer.valueOf(str);
			if(mapoutkey >= 1 && mapoutkey <= 5) {
				return 0;
			}else if (mapoutkey >= 6 && mapoutkey <= 10) {
				return 1;
			}else if (mapoutkey >= 11 && mapoutkey <= 15) {
				return 2;
			}else  {
				return 3;
			}			
			
		}
		
	}
	public static class  WCreducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		//reducer接收数据就是map输出的<key,list[value1,value2,...]
		/**
		 * <Hadoop,list[1,1]>  
		 * <Html,1>
		 * <Java,1>
		 */
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			//定义一个临时变量，用来存放values和
			int sum = 0;
			System.out.print("< "+key+ " -> ");
			//lsit[1,1,1,1....]
			for (IntWritable value: values) {
				sum += value.get();	
				System.out.print(value.get()+",");
			}
			System.out.print(" >"+"\n");
			//将每个单词及其出现的次数，输出到结果文件 part-00000
			context.write(key, new IntWritable(sum));		
		}	
	}
	
	/**
	 * 相当于yarn的客户端，用于向yarn提交当前的mapreduce任务
	 * 可以参考shell 命令行的提交方式
	 * bin/yarn jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar  wordcount /input /output
	 * 这里封装的是自己写的mapreducer程序运行时的参数，同样需要指定jar包，以及输入输出目录
	 * 
	 */
	public int run(String[] args) throws Exception {
		//1.获取Hadoop的配置信息  hdfs-default.xml core-default.xml yarn-defalut.xml mapred-default.xml
		Configuration conf = new Configuration();
		
		
		//2.根据需要可以设置配置信息
		//conf.set(name, value);
		//conf.set("dfs.blocksize", "134217728");
		
		//3.生成对应的job，-> jobname 
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		
		job.setJarByClass(getClass());
		
		//4.设置job的具体的输入目录  map逻辑  reducer逻辑  输出目录
		//4.1 设置输入目录
		Path inputPath = new Path(args[0]);
		/**
		 * InputFormat专门负责处理读取不同类型的数据 
		 */
		FileInputFormat.setInputPaths(job, inputPath); 
	//	FileInputFormat.addInputPath(job, new Path(args[2]));	
		
		//4.2 设置map阶段
		job.setMapperClass(WCmapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Set the number of reduce tasks for the job
		//job.setNumReduceTasks(6);
	//    job.setPartitionerClass(MyPartitioner.class);
		//job.setCombinerClass(MyCombiner.class);
		
		//4.3设置reducer阶段
		job.setReducerClass(WCreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//4.4设置输出目录
		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(conf);
		//设置输出目录如果存在则自动删除
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
//		job.setInputFormatClass(CombineFileInputFormat.class);
//		CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);
//		CombineFileInputFormat.setMinInputSplitSize(job, 2097152);
		
		//5.提交job		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		args = new String[] {
//				"hdfs://ns1/input/wc.txt", //输入目录
//				"hdfs://ns1/output"  //输出目录
//				"E:\\wc.txt",
//				"E:\\mapReduce\\output",
//
//				};
		
		int status = ToolRunner.run(
				conf, 
				new WCmapreduce(), 
				args);
		
		System.exit(status);
	}
	
}
