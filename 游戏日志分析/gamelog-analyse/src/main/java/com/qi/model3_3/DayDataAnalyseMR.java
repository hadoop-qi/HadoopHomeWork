package com.qi.model3_3;

import java.io.IOException;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayDataAnalyseMR {
	
	private static HashSet<String> beforeSet=new HashSet<>();
	private static HashSet<String> todaySet=new HashSet<>();
	
	public static class DayDataAnalyseMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private String fileName = null;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();

			int index = path.lastIndexOf("/");

			fileName = path.substring(index + 1);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			if (fileName.equals("part-r-00002")) {
				
				todaySet.add(value.toString());
				
			} else {
				beforeSet.add(value.toString());
			}

			
		}
	}
	
	public static class DayDataAnalyseReducer extends Reducer<Text, Text, Text, NullWritable>{

		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			int newDevice=0;
			
			for (String string : todaySet) {
				
				if (!beforeSet.contains(string)) {
					newDevice++;
				}
			}
			int oldDevice=todaySet.size()-newDevice;
			
			context.write(new Text(newDevice+"\t"+oldDevice), NullWritable.get());
		}
	}
	
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "DayDataAnalyseMR");
			job.setJarByClass(DayDataAnalyseMR.class);
			
			job.setMapperClass(DayDataAnalyseMapper.class);
			job.setReducerClass(DayDataAnalyseReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			
			Path inputPath = new Path("/DataCut-Output/part-r-00001");
			Path inputPath2 = new Path("/DataCut-Output/part-r-00002");
			Path outPutPath = new Path("/DayDataAnaly-Output");

			FileInputFormat.addInputPath(job, inputPath);
			FileInputFormat.addInputPath(job, inputPath2);
			FileSystem.get(conf).delete(outPutPath, true);
			FileOutputFormat.setOutputPath(job, outPutPath);
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
