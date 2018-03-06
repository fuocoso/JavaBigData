package com.learn.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairWritable implements WritableComparable<PairWritable> {
	private String name; //要比较的第一个字段,姓名
	private float  money; //要比较的第二个字段，消费金额
	

	public PairWritable() {
		
	}
		

	public PairWritable(String name, float money) {
		this.name = name;
		this.money = money;
	}
	
	public void  set(String name, float money) {
		this.name = name;
		this.money = money;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getMoney() {
		return money;
	}

	public void setMoney(float money) {
		this.money = money;
	}
	
	//序列化  将对象转成二进制便于网络传输  
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeFloat(money);
		
	}
	
	//反序列化  注意：字段顺序必须和序列化一致
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		money = in.readFloat();
		
	}
		
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(money);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritable other = (PairWritable) obj;
		if (Float.floatToIntBits(money) != Float.floatToIntBits(other.money))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	//核心的地方 shuffle过程中的排序就是根据此处的比较规则
	public int compareTo(PairWritable o) {
		//先比较二者的name
		int comp = this.name.compareTo(o.name);
		if(0 != comp) {
			return  comp;
		}
		//在第一个字段name一样的情况下比较money,基本数据类型是没有compareTo，需要装箱成包装类
		//return (Float.valueOf(this.money)).compareTo(Float.valueOf(o.money));
		return Float.valueOf(o.money).compareTo(Float.valueOf(this.money));
	}

	
	@Override
	public String toString() {
		return  name +","+ money ;
	}	
	
}
