package com.qi.exam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BlogFansMR2 {

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
	
	public static class BlogFansReducer extends Reducer<Text, Text, Text, NullWritable> {

		HashMap<String, List<String>> map=new HashMap<>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> list=new ArrayList<>();
			for (Text value : values) {
				list.add(value.toString());
			}
			map.put(key.toString(), list);
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

			TreeSet<String> set=new TreeSet<>();
			
			for (String key : map.keySet()) {
				
				List<String> list=map.get(key);
				for (String string : list) {
					if (map.get(string).contains(key)) {
//						int value=Math.abs(key.hashCode()*string.hashCode());
						String value =key.compareTo(string)>0? string+"--"+key : key+"--"+string;
//						cleanupMap.put(String.valueOf(value), string+"--"+key);
						set.add(value);
					}
				}
			}
			
			for (String value : set) {
				context.write(new Text(value), NullWritable.get());
			}
		}
		
	}
	
	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "BlogFansMR2");
			job.setJarByClass(BlogFansMR2.class);
			
			job.setMapperClass(BlogFansMapper.class);
			job.setReducerClass(BlogFansReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/BlogInput");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/BlogOutput2");
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
