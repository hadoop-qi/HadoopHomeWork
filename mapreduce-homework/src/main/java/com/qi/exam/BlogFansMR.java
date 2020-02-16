package com.qi.exam;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.WordWritable;
import com.qi.secondSort.SecondSortMR.SecondGroup;
import com.qi.secondSort.SecondSortMR.SecondSort;

public class BlogFansMR {

	public static class BlogFansMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split(":");
			String users=line[1];
			String fans=line[0];
			String[] usersArray=users.split(",");
			for (String user : usersArray) {
				context.write(new Text(user), new Text(fans));
			}
		}
	}
	
	public static class BlogFansReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb=new StringBuilder();
			for (Text value : values) {
				sb.append(value);
				sb.append(",");
			}
			context.write(new Text(key.toString()+":") , new Text(sb.toString().substring(0,sb.length()-1)));
		}
		
	}
	
	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "BlogFansMR");
			job.setJarByClass(BlogFansMR.class);
			
			job.setMapperClass(BlogFansMapper.class);
			job.setReducerClass(BlogFansReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path inputPath = new Path("/BlogInput");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/BlogOutput");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);
			
			boolean flag = job.waitForCompletion(true);

			System.exit(flag ? 0 : 1);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
}
