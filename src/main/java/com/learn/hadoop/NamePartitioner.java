package com.learn.hadoop;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 如果不做组定义分区：分区使用的是组合key (zhangsan,125.00)
 * 作为优化。希望使用组合key中的name作为分组，这样可以减少key，value对，
 * 而shuffle过程是先分区再分组，所以要将name作为分组，必须使相同name进同一个分区
 * @author Administrator
 *
 */
public class NamePartitioner extends Partitioner<PairWritable, FloatWritable> {

	@Override
	public int getPartition(PairWritable key, FloatWritable value, int numPartitions) {
			
		return (key.getName().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
