package com.learn.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class FilterTest {
	@Test
	public void getAllRow() throws IOException {
		String tbName = "phone_log";

		// 获取配置信息 
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "com.apache.bigdata");

		// 表对象 --》根据指定的表名生成对象
		HTable table = new HTable(conf, tbName);

		// scan
		Scan scan = new Scan();

		// 1.RowFilter 筛选出匹配的所有的行，对于这个过滤器的应用场景
		Filter rFEQ = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("84138413:20130313145955")));
		// scan.setFilter(rFEQ); //等于

		Filter rFGE = new RowFilter(CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("13480253104:20130313145946")));
		Filter rFLE = new RowFilter(CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("18211575961:20130313145953")));
		
		// scan.setFilter(rFGE).setFilter(rFLE);
		 //scan.setFilter(rFLE); //大于等于某个rowkey1 而小于等于另一个rowkey2

		// 2. PrefixFilter：筛选出具有特定前缀的行键的数据。
		Filter pf = new PrefixFilter(Bytes.toBytes("159"));
		 //scan.setFilter(pf);

		// 3.KeyOnlyFilter：这个过滤器唯一的功能就是只返回每行的行键，值全部为空
		Filter kof = new KeyOnlyFilter();
		 //scan.setFilter(kof);

		/**
		 * 4.RandomRowFilter：从名字上就可以看出其大概的用法， 本过滤器的作用就是按照一定的几率（<=0会过滤掉所有的行，>=1会包含所有的行）
		 * 来返回随机的结果集，对于同样的数据集， 多次使用同一个RandomRowFilter会返回不通的结果集， 对于需要随机抽取一部分数据的应用场景
		 */
		Filter rrf = new RandomRowFilter((float) 0.1);
		// scan.setFilter(rrf);

		/**
		 * 5.InclusiveStopFilter：扫描的时候，我们可以设置一个开始行键和一个终止行键，
		 * 默认情况下，这个行键的返回是前闭后开区间，即包含起始行，但不包含终止行， 如果我们想要同时包含起始行和终止行，那么我们可以使用此过滤器：
		 */
		Filter istf = new InclusiveStopFilter(Bytes.toBytes("84138413:20130313145955"));
		 //scan.setStartRow(Bytes.toBytes("18320173382:20130313145946")).setFilter(istf);
		
		/**
		 * 6.FirstKeyOnlyFilter：如果你只想返回的结果集中只包含第一列的数据，那么这个过滤器能够满足你的要求。
		 * 它在找到每行的第一列之后会停止扫描，从而使扫描的性能也得到了一定的提升：
		 */
		Filter fkof = new FirstKeyOnlyFilter();
		// scan.setFilter(fkof);

		/**
		 * 7.ColumnPrefixFilter：顾名思义，它是按照列名的前缀来筛选单元格的， 
		 * 如果我们想要对返回的列的前缀加以限制的话，可以使用这个过滤器：
		 */
		Filter cpf = new ColumnPrefixFilter(Bytes.toBytes("ho"));
		//scan.setFilter(cpf);
		
		/**
		 * 8.ValueFilter：按照具体的值来筛选单元格的过滤器，
		 * 这会把一行中值不能满足的单元格过滤掉，如下面的构造器，
		 * 对于每一行的一个列，如果其对应的值不包含ROW2_QUAL1，那么这个列就不会返回给客户端：
		 */
		Filter vf = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("200"));
		//scan.setFilter(vf);
		scan.setCacheBlocks(true);
		scan.setCaching(500);
		
		/**
		 * scan.setStartRow
		 * scan.setStopRow
		 * 前闭后开
		 */
		 //scan.setStartRow(Bytes.toBytes("18320173382:20130313145946"))
		//.setStopRow(Bytes.toBytes("84138413:20130313145955"));
		// scan.setStopRow(Bytes.toBytes("84138413:20130313145955"));

		// ResultScanner 扫描器
		ResultScanner resultScanner = table.getScanner(scan);

		Iterator<Result> results = resultScanner.iterator();
		while (results.hasNext()) {
			Result result = results.next();
			List<Cell> cells = result.listCells();
			System.err.println(Bytes.toString(result.getRow()));
			for (Cell cell : cells) {
				System.out.println("列簇：" + Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("时间戳：" + cell.getTimestamp());
				System.out.println("列值：" + Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("----分割线 ---");
			}
			
		}
		resultScanner.close();
		table.close();
	}

	
}
