package com.qi.reduceJoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoinMR {

	public static class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();
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

			String[] line = value.toString().split("\\s+");

			if (fileName.equals("record.txt")) {
				outputKey.set(line[0]);
				outputValue.set("record#" + line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[4] + "\t" + line[5]
						+ "\t" + line[6] + "\t" + line[7] + "\t" + line[8] + "\t" + line[9]);
			} else if (fileName.equals("reimburse.txt")) {
				outputKey.set(line[0]);
				outputValue.set("reimburse#" + line[1] + "\t" + line[2]);
			}

			context.write(outputKey, outputValue);
		}

	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputvalue = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context contex)
				throws IOException, InterruptedException {

			ArrayList<String> reimburseString = new ArrayList<>();
			String recordString = null;

			for (Text value : values) {
				
				if (value.toString().startsWith("record#")) {
					recordString = value.toString().substring(7);
				} else if (value.toString().startsWith("reimburse#")) {
					reimburseString.add("\t#\t" + value.toString().substring(10));
				}

			}

			StringBuilder sb = new StringBuilder();

			for (String value : reimburseString) {

				sb.append(value);
			}

			outputKey.set(key.toString() + "\t");
			outputvalue.set(recordString + sb.toString());
			contex.write(outputKey, outputvalue);
		}

	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "ReduceJoinMR");

			job.setJarByClass(ReduceJoinMR.class);

			job.setMapperClass(ReduceJoinMapper.class);
			job.setReducerClass(ReduceJoinReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path inputPath = new Path("/SmartGuide-Input");
			Path outPutPath = new Path("/SmartGuide-Output");
			
			FileSystem.get(conf).delete(outPutPath,true);
			
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outPutPath);

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
