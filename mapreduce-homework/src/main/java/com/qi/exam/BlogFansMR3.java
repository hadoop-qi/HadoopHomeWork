package com.qi.exam;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class BlogFansMR3 {

//	static HashMap<String, String> smallTable=new HashMap<>();
	
	static ArrayList<Map<String,String>> smallTable=new ArrayList<>();
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

		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
						String path = context.getCacheFiles()[0].toString();

						try (FileReader fileReader = new FileReader(path.substring(1));
								BufferedReader bufferedReader = new BufferedReader(fileReader);) {

							String content = null;

							while ((content = bufferedReader.readLine()) != null) {
								HashMap<String, String> map=new HashMap<>();
								String[] words = content.split("--");
								map.put(words[0], words[1]);
								smallTable.add(map);
							}
						}
			
		}
	}
	
	public static class BlogFansReducer extends Reducer<Text, Text, Text, Text> {

		HashMap<String, List<String>> reducemap=new HashMap<>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> list=new ArrayList<>();
			for (Text value : values) {
				list.add(value.toString());
			}
			reducemap.put(key.toString(), list);
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			
			
			for (Map<String, String> map : smallTable) {
				for (String key : map.keySet()) {
					List<String> userAFans=reducemap.get(key);
					List<String> userBFans=reducemap.get(map.get(key));
					StringBuilder sb=new StringBuilder();
					for (String string : userAFans) {
						if (userBFans.contains(string)) {
							sb.append(string);
							sb.append(",");
						}
					}
					context.write(new Text(key+"--"+map.get(key)),new Text(sb.toString().substring(0,sb.length())));
				}
			}
		}
		
	}
	
	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "BlogFansMR3");
			job.setJarByClass(BlogFansMR3.class);
			
			job.setMapperClass(BlogFansMapper.class);
			job.setReducerClass(BlogFansReducer.class);

			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.addCacheFile(new Path("/part-r-00000").toUri());
			Path inputPath = new Path("/BlogInput");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/BlogOutput3");
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
