package com.qi.invertedindex;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	
public static class InvertedIndexMapper extends Mapper<Text, Text, Text, Text>{
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
//			String[] line=value.toString().split("\\s+");
			context.write(value, key);
		}
	}
		
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		private HashSet<String> hs=new HashSet<>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context contex) throws IOException, InterruptedException {
		
			for (Text text : values) {
				hs.add(text.toString());
			}
			
			StringBuffer sb=new StringBuffer();
			for (String value : hs) {
				sb.append("(");
				sb.append(value);
				sb.append("),  ");
			}
			contex.write(key,new Text("\t"+sb.toString().substring(0,sb.length()-3)));
		}
		
	}

	public static void main(String[] args) throws IOException {
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf, "InvertedIndex");
		
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		Path inputPath1=new Path("hdfs://master:9000/input/invertindex");
		Path outPutPath=new Path("hdfs://master:9000/output");
		FileSystem.get(conf).delete(outPutPath,true);
		
		FileInputFormat.addInputPath(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		try {
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
