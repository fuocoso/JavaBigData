package com.learn.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


/**
 * org.apache.hadoop.io.Writable 是hadoop提供的序列化接口 
 * 如果需要自定义key  ->  实现 WritableComparable<T>
 * 如果需要自定义value -> 实现 Writable 
 */
public class FlowBean implements Writable {
	private long upPayLoad;   //上行流量
	private long downPayLoad;  //下行流量
	private long sumPayLoad;   //总流量
		
	public FlowBean() {
		
	}
		
	public FlowBean(long upPayLoad, long downPayLoad, long sumPayLoad) {		
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.sumPayLoad = sumPayLoad;
	}
	
	public void set(long upPayLoad, long downPayLoad, long sumPayLoad) {
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.sumPayLoad = sumPayLoad;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public long getSumPayLoad() {
		return sumPayLoad;
	}

	public void setSumPayLoad(long sumPayLoad) {
		this.sumPayLoad = sumPayLoad;
	}
		
	//序列化过程  对象 -> 二进制
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
		out.writeLong(sumPayLoad);

	}
		
	//反序列化  二进制 -> 对象
	public void readFields(DataInput in) throws IOException {
		upPayLoad = in.readLong();
		downPayLoad = in.readLong();
		sumPayLoad  = in.readLong();

	}

	@Override
	public String toString() {
		return  upPayLoad +";"+ downPayLoad + ";" + sumPayLoad ;
	}
	
	
}
