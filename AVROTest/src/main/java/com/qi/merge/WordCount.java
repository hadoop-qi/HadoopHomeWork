package com.qi.merge;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
	
	public static class WordCountMapper extends Mapper<AvroKey<SmallFile>, NullWritable, Text, IntWritable>{
		
		
		@Override
		protected void map(AvroKey<SmallFile> key, NullWritable value, Mapper<AvroKey<SmallFile>, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
		SmallFile smallFile=key.datum();
		System.out.println(smallFile.getName().toString());
		System.out.println(smallFile.getContent().toString());
			
			
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
//			contex.write(key, new IntWritable(sum));
		}
		
		
		
	}

	public static void main(String[] args) throws IOException {
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf, "AVROwordcount");
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCoundReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		Path inputPath1=new Path("/smallFile.avro");
		Path outPutPath=new Path("/AVRO-output");
		FileSystem.get(conf).delete(outPutPath,true);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, SmallFile.getClassSchema());
	
		FileInputFormat.addInputPath(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outPutPath);
		
		try {
			System.exit(job.waitForCompletion(true)? 0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
