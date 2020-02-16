package com.qi.practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.HeroInfo;
import com.qi.bean.ResultWritable;
import com.qi.practice.GameCalculate.MyMapper;
import com.qi.practice.GameCalculate.MyReducer;

public class TotalSort {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "TotalSort");

		job.setJarByClass(GameCalculate.class);

		job.setMapperClass(TotalSortMapper.class);
		job.setReducerClass(TotalSortReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path inputPath = new Path("hdfs://master:9000/input/numcer.log");
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
	
	
	public static class TotalSortMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

		private LongWritable outputKey=new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			outputKey.set(Integer.parseInt(value.toString()));
			context.write(outputKey, NullWritable.get());
		}
		
	}
	
	public static class TotalSortReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values,
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			int sum=0;
			for (NullWritable nullWritable : values) {
				sum+=1;
			}
//			context.write(key, value);
		}
		
		
		
	}
	
	
	

}
