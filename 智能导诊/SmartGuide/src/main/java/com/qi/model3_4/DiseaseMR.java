package com.qi.model3_4;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiseaseMR {

	public static class DiseaseMapper extends Mapper<LongWritable, Text, Text, Text>{

		private static Text outputKey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split("\\s+");
			String diseaseId=line[2];
			String hospitalId=line[1];
			outputKey.set(diseaseId+"\t"+hospitalId);
			context.write(outputKey, value);
		}
	}
	
	public static class DiseaseReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			Long treatCount=0L;
			Long cureCount=0L;
			for (Text value : values) {
				treatCount++;
				String treatResult= value.toString().split("\\s+")[9];
				if ("0".equals(treatResult)) {
					cureCount++;
				}
			}
//			System.out.println("treatCount:"+treatCount+"\t"+"cureCount:"+cureCount);
			
			Double cureRate=cureCount*1.0/treatCount;
			BigDecimal bd = new BigDecimal(cureRate);  
			cureRate = bd.setScale(5, RoundingMode.HALF_UP).doubleValue();
			
			context.write(key, new Text("\t"+treatCount+"\t"+cureRate));
		}
		
	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "DiseaseMR");

			job.setJarByClass(DiseaseMR.class);

			job.setMapperClass(DiseaseMapper.class);
			job.setReducerClass(DiseaseReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path inputPath = new Path("/SmartGuide-Input/record.txt");
			Path outPutPath = new Path("/Model3_4-midOutput");
			
			FileSystem.get(conf).delete(outPutPath,true);
			
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outPutPath);

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
