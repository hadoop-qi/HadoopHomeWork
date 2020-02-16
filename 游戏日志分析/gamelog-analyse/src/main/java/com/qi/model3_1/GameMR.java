/**
 * 
 */
package com.qi.model3_1;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Administrator
 *
 */
public class GameMR {
	
	
	public static class GameMapper extends Mapper<LongWritable, Text, Text, Text>{

		private static Text outputKey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line= value.toString().split("\\s+");
			String device=line[0];
			outputKey.set(device);
			context.write(outputKey, value);
		}
	}
	
	public static class GameReducer extends Reducer<Text, Text, Text, NullWritable>{

		int totalDevice=0;
		int totalPlay=0;
		long totalDuration=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			totalDevice++;
			
			for (Text value : values) {
				totalPlay++;
				String[] line= value.toString().split("\\s+");
				totalDuration+=Long.parseLong(line[5]);
			}
			
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
		
			double avgDeviceDuration=totalDuration*1.0/totalDevice;
			BigDecimal bd = new BigDecimal(avgDeviceDuration);  
			avgDeviceDuration = bd.setScale(2, RoundingMode.HALF_UP).doubleValue();
			
			double avgPlayDuration=totalDuration*1.0/totalPlay;
			BigDecimal bd2 = new BigDecimal(avgPlayDuration);  
			avgPlayDuration = bd2.setScale(2, RoundingMode.HALF_UP).doubleValue();
			
			context.write(new Text(totalDevice+"\t"+totalPlay+"\t"+avgDeviceDuration+"\t"+avgPlayDuration), NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "GameMR");
			job.setJarByClass(GameMR.class);

			job.setMapperClass(GameMapper.class);
			job.setReducerClass(GameReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/GameInput");
			Path outPutPath = new Path("/GameOutput");

			FileInputFormat.addInputPath(job, inputPath);
			FileSystem.get(conf).delete(outPutPath, true);
			FileOutputFormat.setOutputPath(job, outPutPath);
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

}
