package com.qi.topN;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

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

public class TopN {
	
	public static enum WordCount {
		Line,words;
	}
	
	public static class MyMApper extends Mapper<LongWritable, Text, Text, Text>{

		TreeMap<Long, String> map =new TreeMap<>(new Comparator<Long>() {

			@Override
			public int compare(Long o1, Long o2) {
				
				return -o1.compareTo(o2);
			}
		});
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split("\\s+");
			String word=line[0];
			String fileName=line[1].split("---")[0];
			long num=Long.parseLong(line[1].split("---")[1]);
			map.put(num, word+";"+fileName);
			if (map.size()==11) {
				map.remove(map.lastKey());
			}
			
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (Long key : map.keySet()) {
				context.write(new Text(map.get(key)), new Text(String.valueOf(key)));
			}
			
		}
		
		
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		TreeMap<Long, String> map =new TreeMap<>(new Comparator<Long>() {

			@Override
			public int compare(Long o1, Long o2) {
				
				return -o1.compareTo(o2);
			}
		});
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (Text text : values) {
				map.put(Long.parseLong(text.toString()),key.toString());
				if (map.size()==11) {
					map.remove(map.lastKey());
				}
			}
			
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			for (Long key : map.keySet()) {
				
				String word=map.get(key).split(";")[0];
				String fileName=map.get(key).split(";")[1];
				context.write(new Text(word), new Text(fileName+"---"+key));
			}
			
		}
		
		
	}
	
	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "topN");
			job.setJarByClass(TopN.class);
			
			job.setMapperClass(MyMApper.class);
			job.setReducerClass(MyReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path inputPath = new Path("/topNInput/word_file_count");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/topNOutput");
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
