package com.learn.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class User2StudentMapReduce extends Configured implements Tool{

	// 使用TableMapper读取HBase表的数据
	public static class ReadUserMapper extends
			TableMapper<ImmutableBytesWritable, Put> {

		//读取user表，每行作为一个输入，并取出了rowkey
		protected void map(ImmutableBytesWritable key, Result row,
				Context context) throws IOException, InterruptedException {
			
			Put put = new Put(key.get());
			for (Cell cell : row.rawCells()) {
				if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
					if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						put.add(cell);//将info:name列放入put
						// CellUtil.cloneValue(cell)
						// put.add(family, qualifier, value) ;
					}
					else if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						put.add(cell);//将info:age列放入put
					}
				}
			}
			// mapper output
			context.write(key, put);
		}
	}

	public static class WriteStudentReducer extends
			TableReducer<ImmutableBytesWritable, Put, NullWritable> {

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Put> puts,
				Context context) throws IOException, InterruptedException {
			for (Put put : puts) {
				// reducer output
				context.write(NullWritable.get(), put);
			}
		}
	}

	//运行
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());//job名任意
		job.setJarByClass(User2StudentMapReduce.class);
		job.setNumReduceTasks(1); //reducer个数

		Scan scan = new Scan();
		scan.setCacheBlocks(false); //MR的时候为非热点数据，不需要缓存
		scan.setCaching(500); //每次从服务器端读取的行数

		TableMapReduceUtil.initTableMapperJob("emp", //输入表
				scan,
				ReadUserMapper.class, // mapper class
				ImmutableBytesWritable.class, // mapper output key
				Put.class, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob("student", //输出表
				WriteStudentReducer.class, // reducer class
				job);

		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int status = ToolRunner.run(conf, new User2StudentMapReduce(), args);
		System.exit(status);
	}
}