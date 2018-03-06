package com.learn.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortMR extends Configured implements Tool {
	/**
	 * LongWritable Text IntWritable这是都是HDFS的特有的数据类型，都实现了Hadoop的序列化接口Writable
	 * LongWritable Text IntWritable直接实现的WritableComparable，带有比较功能
	 * @author Administrator
	 */
	
	public static class  SecondarySortmapper extends Mapper<LongWritable, Text, PairWritable, FloatWritable>{
		/**
		 * zhangsan 120.75
		 */
		private  PairWritable mapOutkey = new PairWritable();
		private  FloatWritable  MapOutValue =new FloatWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PairWritable, FloatWritable>.Context context)
				throws IOException, InterruptedException {
		
			//1.将每一行的内容转成String
			String line = value.toString();
			
			//2.将String类型的每一行内容转成Sting[] str=[zhangsan,120.75]
			String[] str = line.split(" ");
			
			//3.将原来的每一行的name和money切分并组织成自定义的key，作为map输出的key
			mapOutkey.set(str[0], Float.parseFloat(str[1]));
			
			//4.将原来的金额money这一列作为map输出的value
			MapOutValue.set(Float.parseFloat(str[1]));
			
			context.write(mapOutkey, MapOutValue);				
		}		
	}
			
	public static class  SecondarySortreducer extends Reducer<PairWritable, FloatWritable, Text, FloatWritable>{
		//reducer接收数据就是map输出的<key,list[value1,value2,...]
		/**
		 * <zhangsan,120.75 list[120.75]>  
		 */	
			
		private Text outputKey = new Text();
		private FloatWritable outputValue = new FloatWritable();
		
		@Override
		protected void reduce(PairWritable key, Iterable<FloatWritable> values,
				Reducer<PairWritable, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		System.out.print("key: ->" + key+" value:-> List [ ");
		
			//1.从组合key中通过getName()获取姓名，作为整体输出的key	
		outputKey.set(key.getName());
						
			//lsit[120.75]
			for (FloatWritable value: values) {					
				context.write(outputKey, value);
				System.out.print(value+",");
			}	
			System.out.print(" ]"+"\n");
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
		
		
		//4.2 设置map阶段
		job.setMapperClass( SecondarySortmapper.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
						
	 //   job.setPartitionerClass(NamePartitioner.class);
	//    job.setGroupingComparatorClass(NameGroupping.class);

		
		//4.3设置reducer阶段
		job.setReducerClass( SecondarySortreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		
		
		//4.4设置输出目录
		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(conf);
		//设置输出目录如果存在则自动删除
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		FileOutputFormat.setOutputPath(job, outputPath);
		

		
		//5.提交job		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new String[] {
				"hdfs://bigdata.Linux1/input/Secondarysort.txt", //输入目录
				"hdfs://bigdata.Linux1/output"  //输出目录
				};
		
		int status = ToolRunner.run(
				conf, 
				new SecondarySortMR(), 
				args);
		
		System.exit(status);
	}
	
}
