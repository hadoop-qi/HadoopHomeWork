package com.qi.basic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text outputKey=new Text();
		private static IntWritable outputvalue=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st=new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				outputKey.set(st.nextToken());
				context.write(outputKey, outputvalue);
			}
		}
		
		
	}
		
	public static class WordCoundReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context contex) throws IOException, InterruptedException {
			
			int sum=0;
			for (IntWritable intWritable : value) {
				
				sum++;
			}
			contex.write(key, new IntWritable(sum));
		}
		
		
		
	}

	public static void main(String[] args) throws IOException {
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCoundReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path inputPath1=new Path("hdfs://master:9000/input/game.log");
		Path inputPath2=new Path("hdfs://master:9000/input/game_result1");
		Path outPutPath=new Path("hdfs://master:9000/output");
		FileSystem.get(conf).delete(outPutPath,true);
		
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		CombineTextInputFormat.setMaxInputSplitSize(job, 10*1024);//10k
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.RECORD);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		FileInputFormat.addInputPath(job, inputPath1);
		FileInputFormat.addInputPath(job, inputPath2);
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		try {
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
