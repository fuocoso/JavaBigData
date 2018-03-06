package com.learn.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sound.midi.VoiceStatus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class HBaseAPI {
	/**
	 * 1.创建表格 create 'namespace:tablename','cf1','cf2'
	 * 
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	@Test
	public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		String tbName = "ns1:person";
		String colFamily = "info";
		/**
		 * 获取配置文件对象 ->hbase-common-0.98.6-hadoop2.jar
		 */
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// Hbase集群管理员对象，-》对比MySQL:root->管理员用户-》也是对象
		HBaseAdmin admin = new HBaseAdmin(config);

		// 创建一个表格名称对象
		TableName tableName = TableName.valueOf(tbName);

		// 相当于HBase shell中的 describe
		HTableDescriptor hTableDesc = new HTableDescriptor(tableName);

		hTableDesc.addFamily(new HColumnDescriptor(colFamily));
		hTableDesc.addFamily(new HColumnDescriptor("job"));

		// 通过HBaseAdmin创建表表
		admin.createTable(hTableDesc);
		admin.close();
		System.out.println(tbName + "已经成功创建出来了");
	}

	/**
	 * 2.插入数据 put 'namespache:tablename','rokey','columnfamily:column','value'
	 * 
	 * @throws IOException
	 */
	@Test
	public void putData() throws IOException {
		String tbName = "ns1:person";
		String colFamily = "info";
		String rowKey = "1001a";
		String column = "name";
		String value = "zhangsan";
		/**
		 * client -> Zookeeper(meta-regionser) -> hbase:meta ->
		 */
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// 具体的表格对象
		HTable table = new HTable(config, tbName);

		// 获取当前该表的所有的列族
		HColumnDescriptor[] columnFamiles = table.getTableDescriptor().getColumnFamilies();

		// 构建put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		// Put put2 = new Put(Bytes.toBytes("1001b"));
		List<Put> puts = new ArrayList<Put>();

		for (int i = 0; i < columnFamiles.length; i++) {
			// 以字符串类型获取该表当前所有的列族
			String familyName = columnFamiles[i].getNameAsString();
			// 判断要插入数据的列族在该表中是否存在，存在才可以插入
			if (colFamily.equals(familyName)) {
				put.add(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
				put.add(Bytes.toBytes(familyName), Bytes.toBytes("age"), Bytes.toBytes("17"));
				put.add(Bytes.toBytes(colFamily), Bytes.toBytes("birthday"), Bytes.toBytes("1992-10-01"));
			}
		}

		table.put(put);
		// table.put(put2);
		table.setAutoFlushTo(false);
		//table.flushCommits();
	
		// puts.add(put);
		// puts.add(put2);
		// table.put(puts); //推荐用法
		System.out.println("数据插入成功");
	}

	/**
	 * 3.查询数据 :get get 'namespace:tablename','rowkey'
	 * 
	 * @throws IOException
	 */
	@Test
	public void getRow() throws IOException {
		String tbName = "ns1:person";
		String rowKey = "1001a";

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// 具体的表格对象
		HTable table = new HTable(config, tbName);

		Get get = new Get(Bytes.toBytes(rowKey));

		// 获取对应表格的某一行具体的结果集
		Result res = table.get(get);

		List<Cell> listCells = res.listCells();

		for (Cell cell : listCells) {
			System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("时间戳：" + cell.getTimestamp());
			System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("------分割线-------");
		}
	}

	/**
	 * 4.全表扫描，获取全表的数据 scan 'namespace:tablename'
	 * 
	 * @throws IOException
	 */
	@Test
	public void scanTable() throws IOException {
		String tbName = "ns1:person";

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// 具体的表格对象
		HTable table = new HTable(config, tbName);

		// ResultScann
		ResultScanner reScanner = table.getScanner(new Scan());

		Iterator<Result> results = reScanner.iterator();

		while (results.hasNext()) {
			Result result = results.next();
			List<Cell> listCells = result.listCells();

			for (Cell cell : listCells) {
				System.out.println(Bytes.toString(result.getRow()));
				System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("时间戳：" + cell.getTimestamp());
				System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("------分割线-------");
			}
		}
	}
	
	/**
	 * 5.删除单元格 
	 * delete 'namesapce:tablename','rowkey','columnfamiley:column'
	 * @throws IOException 
	 */
	@Test
	public void deleteCell() throws IOException {
		String tbName = "ns1:person";
		String colFamily = "info";
		String rowKey = "1001a";
		String column = "name";
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// 具体的表格对象
		HTable table = new HTable(config, tbName);
		
		//Delete 对象
		Delete del = new Delete(Bytes.toBytes(rowKey));
		del.deleteColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column));
		
		table.delete(del);
		table.close();
		System.out.println("rowKey是"+rowKey+"的列族"+colFamily+"下的"+column+"被成功的删除了");
	}
	
	/**
	 * 6.删除整行数据
	 * @throws IOException 
	 */
	public void deleteRow() throws IOException {
		String tbName = "ns1:person";
		String colFamily = "info";
		String rowKey = "1001a";
		String column = "name";
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// 具体的表格对象
		HTable table = new HTable(config, tbName);
		
		//Delete 对象
		Delete del = new Delete(Bytes.toBytes(rowKey));	
		table.delete(del);
		
		table.close();
		System.out.println("rowKey是"+rowKey+"的列族"+colFamily+"下的"+column+"被成功的删除了");
	}
	
	/**
	 * drop  disable enable
	 * drop 'namespace:tableName'
	 * disable  'namespace:tableName'
	 * enable 'namespace:tableName'
	 * @throws Exception 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	@Test
	public void managerTable() throws MasterNotRunningException, ZooKeeperConnectionException, Exception {
		String tbName = "ns1:person";
		/**
		 * 获取配置文件对象 ->hbase-common-0.98.6-hadoop2.jar
		 */
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "com.apache.bigdata:2181");

		// Hbase集群管理员对象，-》对比MySQL:root->管理员用户-》也是对象
		HBaseAdmin admin = new HBaseAdmin(config);
		
//		admin.disableTable(tbName);
//		admin.enableTable(tbName);
//		admin.deleteTable(tbName);
//		admin.flush(tbName);
		admin.compact(tbName);
//		admin.split(tbName);
	}
}
