package com.qi.model3_4;

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

public class FinalDiseaseMR {

	public static class FinalDiseaseMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			context.write(value, value);
		}
	}

	public static class FinalDiseaseReducer extends Reducer<Text, Text, DiseaseWritable, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, DiseaseWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for (Text value : values) {
//				System.out.println("===========key="+key.toString()+"\tvalue="+value.toString()+"=================");
				count++;
				if (count > 5) {
					break;
				}
				
				String[] diseaseInfoArray = value.toString().split("\\s+");
				DiseaseWritable diseaseWritable = new DiseaseWritable();

				diseaseWritable.setDiseaseId(diseaseInfoArray[0]);
				diseaseWritable.setHospitalId(diseaseInfoArray[1]);
				diseaseWritable.setTreatCount(Long.parseLong(diseaseInfoArray[2]));
				diseaseWritable.setCureRate(Double.parseDouble(diseaseInfoArray[3]));
				context.write(diseaseWritable, NullWritable.get());
			}
			System.out.println("\n\n\n");

		}

	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/smart_guide", "root",
					"123456");

			Job job = Job.getInstance(conf, "FinalDiseaseMR");

			job.setJarByClass(FinalDiseaseMR.class);

			job.setMapperClass(FinalDiseaseMapper.class);
			job.setReducerClass(FinalDiseaseReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(DiseaseWritable.class);
			job.setOutputValueClass(NullWritable.class);

			job.setSortComparatorClass(DiseaseSort.class);
			job.setGroupingComparatorClass(DiseaseGroup.class);

			Path inputPath = new Path("/Model3_4-midOutput");
			// Path outPutPath = new Path("/Model3_4-finalOutput");

			// FileSystem.get(conf).delete(outPutPath,true);

			FileInputFormat.addInputPath(job, inputPath);
			// FileOutputFormat.setOutputPath(job, outPutPath);
			DBOutputFormat.setOutput(job, "disease_info", "diseaseId", "hospitalId", "treatCount", "cureRate");

			job.addFileToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
