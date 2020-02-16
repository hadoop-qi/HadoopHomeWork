package com.qi.practice;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Administrator
 * 3. 每个 ip 的每个操作 每天执行了多少次
    192.168.10.109--remove   25
    192.168.10.109--login    3
    192.168.13.129--upload   10
    192.168.13.129--create   2
 */
public class Practice3 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outputKey = new Text();
		private static IntWritable outputvalue = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			StringTokenizer st = new StringTokenizer(value.toString(),"\n");
			while (st.hasMoreTokens()) {
				String nextToken = st.nextToken();
				
					outputKey.set(nextToken);
					context.write(outputKey, outputvalue);
				
				
			}
		}

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context contex) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : value) {

				sum++;
			}
			contex.write(new Text(key.toString()+"--"), new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "practice3");

		job.setJarByClass(Practice3.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path inputPath = new Path("hdfs://master:9000/input/zy_cloud_disk.log");
		Path outPutPath = new Path("hdfs://master:9000/output");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPutPath);

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
