package com.qi.model3_2;

import java.io.IOException;

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

public class DataCutMR {
	
	public static class DataCutMapper extends Mapper<LongWritable, Text, Text, Text>{

		private static Text outputKey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split("\\s+");
			String device=line[0];
			outputKey.set(device);
			context.write(outputKey, value);
		}
	}
	
	public static class DataCutReducer extends Reducer<Text, Text, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context contex)
				throws IOException, InterruptedException {
			
			contex.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "DataCutMR");
			job.setJarByClass(DataCutMR.class);
			
			job.setNumReduceTasks(7);
			job.setPartitionerClass(DataPartionner.class);

			job.setMapperClass(DataCutMapper.class);
			job.setReducerClass(DataCutReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			
			Path inputPath = new Path("/GameInput");
			Path outPutPath = new Path("/DataCut-Output");

			FileInputFormat.addInputPath(job, inputPath);
			FileSystem.get(conf).delete(outPutPath, true);
			FileOutputFormat.setOutputPath(job, outPutPath);
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
