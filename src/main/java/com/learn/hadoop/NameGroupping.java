package com.learn.hadoop;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class NameGroupping implements RawComparator<PairWritable> {
	
	//在对象层面上对组合key(PairWritable)中的第一个字段name进行比较，不是很高效
	public int compare(PairWritable p1, PairWritable p2) {		
		return p1.getName().compareTo(p2.getName());
	}
	
	//相对于上述方式，这是一种在二进制也就是在字节数组上进行的比较，会更加高效
	/**
	 *比如zhangsan,125.00 -> 
	 *string,float  -> 'z' 'h' 'a' 'n' 'g' 's' 'a' 'n' A,B,C,5
	 *lisi,65.00 -> 'l' 'i' 's' 'i' 1,5,3,7
	 */
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, 0, l1-4, b2, 0, l2-4);
	}

}
