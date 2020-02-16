package com.qi.topN;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;

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

public class GroupTopN {
	
	
	
	public static class MyMApper extends Mapper<LongWritable, Text, Text, Text>{

		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String word=value.toString().split("\\s+")[0];
			context.write(new Text(word), value);
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		TreeSet<String> set=new TreeSet<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				Integer num1=Integer.parseInt(o1.split("---")[1]);
				Integer num2=Integer.parseInt(o2.split("---")[1]);
				return -num1.compareTo(num2);
			}
		});
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (Text text : values) {
				set.add(text.toString());
				if (set.size()==4) {
					set.remove(set.last());
				}
			}
			for (String value : set) {
				context.write(new Text(value), new Text());
				
			}
			set.clear();
		}
		
	}
	
	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "topN");
			job.setJarByClass(GroupTopN.class);
			
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
