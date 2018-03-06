package com.learn.hbase;  
  
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.TableName;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.Mutation;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;  
import org.apache.hadoop.hbase.mapreduce.TableReducer;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.NullWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.IOException;  
import java.text.SimpleDateFormat;  
import java.util.Date;  
  
  
/** 
 * Created by Administrator on 2017/3/7. 
 */  
public class LoadData extends Configured implements Tool {  
    public static class LoadDataMapper extends Mapper<LongWritable,Text,LongWritable,Text>{  
        private Text out = new Text();  
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");  
  
        @Override  
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            //1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
        	String line = value.toString();  
            String [] splited = line.split("\t");  
            String  formatedDate = simpleDateFormat.format(new Date(Long.parseLong(splited[0].trim())));  
            String rowKeyString = splited[1]+":"+formatedDate;  
            out.set(rowKeyString+"\t"+line);  
            //13726230503:201706291728	1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
            context.write(key,out);  
        }  
    }  
    public static class LoadDataReducer extends TableReducer<LongWritable,Text,NullWritable>{  
        public static final String COLUMN_FAMILY = "cf";  
        @Override  
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {  
  
            for (Text tx : values) {  
                String[] splited = tx.toString().split("\t");  
                String rowkey = splited[0];  
  
  
                Put put = new Put(rowkey.getBytes());  
//                put.add(COLUMN_FAMILY.getBytes(), "raw".getBytes(), tx  
//                        .toString().getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "reportTime".getBytes(),  
                        splited[1].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "msisdn".getBytes(),  
                        splited[2].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "apmac".getBytes(),  
                        splited[3].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "acmac".getBytes(),  
                        splited[4].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "host".getBytes(),  
                        splited[5].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "siteType".getBytes(),  
                        splited[6].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "upPackNum".getBytes(),  
                        splited[7].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "downPackNum".getBytes(),  
                        splited[8].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "upPayLoad".getBytes(),  
                        splited[9].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "downPayLoad".getBytes(),  
                        splited[10].getBytes());  
                put.add(COLUMN_FAMILY.getBytes(), "httpStatus".getBytes(),  
                        splited[11].getBytes());  
                context.write(NullWritable.get(), put);  
            }  
        }  
    }  
    public static void createHBaseTable(String tableName) throws IOException {  
 
        HTableDescriptor htd = new HTableDescriptor(  
                TableName.valueOf(tableName));  
        HColumnDescriptor col = new HColumnDescriptor("cf");  
        htd.addFamily(col);  
        Configuration conf = HBaseConfiguration.create();  
        conf.set("hbase.zookeeper.quorum", "com.apache.bigdata");  
        HBaseAdmin admin = new HBaseAdmin(conf);  
        if (admin.tableExists(tableName)) {  
            System.out.println("table exists, trying to recreate table......");  
            admin.disableTable(tableName);  
            admin.deleteTable(tableName);  
        }  
        System.out.println("create new table:" + tableName);  
        admin.createTable(htd);  
          
    }  
    public int run(String[] args) throws Exception {
    	Configuration conf = HBaseConfiguration.create();  
        // conf.set("hbaser.rootdir","hdfs://bigdata:8020/hbase");  
        conf.set("hbase.zookeeper.quorum", "com.apache.bigdata"); 
        
        conf.set(TableOutputFormat.OUTPUT_TABLE, "phone_log"); 
        
        createHBaseTable("phone_log"); 
        
        Job job = Job.getInstance(conf, "LoadData");  
        job.setJarByClass(LoadData.class);  
        job.setNumReduceTasks(1);  
    
        // 3.2 map class  
        job.setMapperClass(LoadDataMapper.class);  
        job.setMapOutputKeyClass(LongWritable.class);  
        job.setMapOutputValueClass(Text.class);  
   
        // 3.3 reduce class  
        job.setReducerClass(LoadDataReducer.class);  
      //  job.setOutputKeyClass(NullWritable.class);     --不需要设置  
     //   job.setOutputValueClass(Mutation.class);     --不需要设置  
         
        Path inPath = new Path(args[0]);  
        FileInputFormat.addInputPath(job, inPath);  
        
        job.setOutputFormatClass(TableOutputFormat.class);  
   
       boolean isSucced = job.waitForCompletion(true);
       
		return isSucced ? 0 : 1; 
	}  
    public static void main(String[] args) throws Exception { 
    	Configuration conf = HBaseConfiguration.create();     	
       args = new String[] { "hdfs://com.apache.bigdata:8020/input" };  
       
       int status = ToolRunner.run(conf, 
    		   new LoadData(), 
    		   args);
         
  System.exit(status);
    }
	
}  