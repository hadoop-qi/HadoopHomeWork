package com.qi.wordcount;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.avrobean.Employee2;

public class EmployWordCount {
	
	public static class WordCountMapper extends Mapper<AvroKey<Employee2>, NullWritable,IntWritable, IntWritable>{
		
		private static IntWritable outputKey=new IntWritable();
		private static IntWritable outputValue=new IntWritable(1);
		
		@Override
		protected void map(AvroKey<Employee2> key, NullWritable value, Mapper<AvroKey<Employee2>, NullWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			

			Employee2 employee2 = key.datum();
			
			outputKey.set(employee2.getDepartId());
			
			context.write(outputKey, outputValue);
		
		}
		
	}
	
	public static class WordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context conetx)
				throws IOException, InterruptedException {
		
			int sum=0;
			for (IntWritable intWritable : values) {
				
				sum+=intWritable.get();
			}
			conetx.write(key, new IntWritable(sum));
			
		}
		
		
	}
		

	public static void main(String[] args) throws IOException {
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf, "employcount");
		
		job.setJarByClass(EmployWordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, Employee2.getClassSchema());
		
		Path inputPath=new Path("/employFile-output");
		Path outPutPath=new Path("/employCount-output");
		FileSystem.get(conf).delete(outPutPath,true);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		try {
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
