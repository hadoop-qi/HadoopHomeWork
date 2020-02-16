package com.qi.wordcount;

import java.io.IOException;

import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.avrobean.SmallFile;
import com.qi.avrobean.WordCountResult;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<AvroKey<SmallFile>, NullWritable, Text, IntWritable>{
		
		private static Text outputKey=new Text();
		
		private static IntWritable outputValue=new IntWritable();
		
		@Override
		protected void map(AvroKey<SmallFile> key, NullWritable value, Mapper<AvroKey<SmallFile>, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			

			SmallFile smallFile = key.datum();
			
			String content = smallFile.getContent().toString();
			
			String[] lines = content.split("\\n");
			
			for (String row : lines) {
				
				String[] words = row.split("\\s+");
				
				for (String word : words) {
					
					outputKey.set(word);
					
					context.write(outputKey, outputValue);
				}
			}
		
		}
		
		
	}
		
	public static class WordCoundReducer extends Reducer<Text, IntWritable, AvroKey<WordCountResult>, NullWritable> {

		private static AvroKey<WordCountResult> outputKey=new AvroKey<>();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, AvroKey<WordCountResult>, NullWritable>.Context contex) throws IOException, InterruptedException {
			
			int sum=0;
			for (IntWritable intWritable : value) {
				
				sum++;
			}
			WordCountResult wordCountResult=new WordCountResult();
			wordCountResult.setCount(sum);
			wordCountResult.setWord(key.toString());
			outputKey.datum(wordCountResult);
			contex.write(outputKey, NullWritable.get());
			
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
		
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		AvroJob.setInputKeySchema(job, SmallFile.getClassSchema());
		AvroJob.setOutputKeySchema(job, WordCountResult.getClassSchema());
		
		Path inputPath=new Path("/AVRO-input/small_file.avro");
		Path outPutPath=new Path("/AVRO-output");
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
