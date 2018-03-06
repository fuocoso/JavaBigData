package com.learn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/3/27.
 */
public class dataJoinTotal extends Configured implements Tool {

    public static class WordMapper extends Mapper<LongWritable, Text, IntWritable, DataJoinWritable> {
        private IntWritable mapOutputKey = new IntWritable();
        private final DataJoinWritable mapOutputValue = new DataJoinWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();
            //获取map输入的split切片对象
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            //根据切片对象获取切片文件的路径名称
            String inPathName = inputSplit.getPath().getName();

            //根据路径名称来判断文件归属
            if(inPathName.contains("customer")){
                String[] strs = lineValue.split(",");
                String s1 = strs[1] +","+strs[2];
                mapOutputKey.set(Integer.parseInt(strs[0]));
                mapOutputValue.set("customer_", s1);
                context.write(mapOutputKey, mapOutputValue);
            } else{
                String[] strs = lineValue.split(",");
                String s2 = strs[1] +","+ strs[2] +","+ strs[3];
                mapOutputKey.set(Integer.parseInt(strs[0]));
                mapOutputValue.set("order_", s2);
                context.write(mapOutputKey, mapOutputValue);
            }

//            String[] strs = lineValue.split(",");
//            mapOutputKey.set(Integer.valueOf(strs[0]));
//            if (3 == strs.length) {
//                String s1 = strs[1] + strs[2];
//                mapOutputValue.set("customer_", s1);
//            } else {
//                String s2 = strs[1] + strs[2] + strs[3];
//                mapOutputValue.set("order_", s2);
//            }
 //           context.write(mapOutputKey, mapOutputValue);
        }
    }


    public static class WordReduce extends Reducer<IntWritable, DataJoinWritable, IntWritable, Text> {
        Text v = new Text();

        //@Override
        protected void reduce(IntWritable key, Iterable<DataJoinWritable> values, Context context) throws IOException, InterruptedException {
            String customerInfo = null;
            //orderList存放order表的记录
            List<String> orderList = new ArrayList<String>();
            System.out.print("value->"+"[ ");
            
            for (DataJoinWritable value : values) {
                System.out.print(value.getData() + " ");
                if ("customer_".equals(value.getTag())) {
                    customerInfo = value.getData();
                } else {
                    orderList.add(value.getData());
                }

            }
            /**
             * join过程
             */
            for (String order : orderList) {
                if (customerInfo != null) {
                    v.set(customerInfo.toString() + " , " + order.toString());
                    context.write(key, v);
                }
            }
            System.out.print(" ]"+"\n");
        }

    }


    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Path inPath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, inPath);

        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataJoinWritable.class);

        job.setReducerClass(WordReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        Path outPath = new Path(args[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);
        boolean inSuccess = job.waitForCompletion(true);

        return inSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


//        args = new String[]{"E:\\mrjoin",
//                "E:\\mrjoinoutput"};

        // run job
        int status = ToolRunner.run(
                conf,
                new dataJoinTotal(),
                args);

        // exit program
        System.exit(status);
    }
}
