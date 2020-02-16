package com.qi.practice;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.IpActionWriable;

/**
 * @author Administrator
 * 3. 每个 ip 的每个操作 每天执行了多少次
    192.168.10.109--remove   25
    192.168.10.109--login    3
    192.168.13.129--upload   10
    192.168.13.129--create   2
 */
public class Practice3 {
	public static class MyMapper extends Mapper<LongWritable, Text, IpActionWriable, IntWritable> {

		
		private static IntWritable outputvalue = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IpActionWriable, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] strArray=value.toString().split("\\s+");
			IpActionWriable ipAction=new IpActionWriable();
			ipAction.setIp(strArray[0]);
			ipAction.setAction(strArray[1]);
			context.write(ipAction, outputvalue);
			
		}

	}

	public static class MyReducer extends Reducer<IpActionWriable, IntWritable, IpActionWriable, IntWritable> {

		
		@Override
		protected void reduce(IpActionWriable key, Iterable<IntWritable> value,
				Reducer<IpActionWriable, IntWritable, IpActionWriable, IntWritable>.Context contex) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : value) {

				sum++;
			}
			contex.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "practice3");

		job.setJarByClass(Practice3.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(IpActionWriable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IpActionWriable.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath = new Path("hdfs://master:9000/input/zy_cloud_disk.log");
		Path outPutPath = new Path("hdfs://master:9000/output");
		FileSystem.get(conf).delete(outPutPath,true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPutPath);

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
