package com.qi.model3_3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;

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

public class OutPationMR {

	public static class OutpationMapper extends Mapper<LongWritable, Text, Text, Text>{

		private static Text outputKey=new Text();
		private static Text outputValue=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split("#");
			String record=line[0];
			String[] recordArray=line[0].split("\\s+");
			String hospital_id=recordArray[1];
			if ("2".equals(recordArray[5])) {
				outputKey.set(hospital_id);
				outputValue.set(value.toString());
				context.write(outputKey, outputValue);
			}
		}
	}
	
	
	public static class OutpationReducer extends Reducer<Text, Text, OutPationWritable,NullWritable> {

		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, OutPationWritable,NullWritable>.Context contex)
				throws IOException, InterruptedException {
			
			OutPationWritable outPationWritable=new OutPationWritable();
			Long outPationPeople=0L;
			Long curePeople=0L;
			Double totalCost=0.0;
			ArrayList<String> totalReimburse=new ArrayList<>();
			for (Text value : values) {
				outPationPeople++;
				String[] outpationValue=value.toString().split("#");
				String record=outpationValue[0];
				
				for (int i = 1; i < outpationValue.length; i++) {
					
					totalReimburse.add(outpationValue[i]);
				}
				
				String[] recordArray=record.split("\\s+");
				
				totalCost+=Double.parseDouble(recordArray[8]);
				if ("0".equals(recordArray[9])) {
					curePeople++;
				}
			}
			
			outPationWritable.setTotalPeople(outPationPeople);
			
			Double averageCost=totalCost/outPationPeople;
			BigDecimal bd = new BigDecimal(averageCost);  
			averageCost = bd.setScale(2, RoundingMode.HALF_UP).doubleValue(); 
			outPationWritable.setAverageCost(averageCost);
			
			Double totalSubmit=0.0;
			for (String value : totalReimburse) {
				value=value.trim();
				String[] reimburseArray=value.split("\\s+");
				totalSubmit+=Double.parseDouble(reimburseArray[1]);
			}
			
			Double averageSubmit=totalSubmit/outPationPeople;
			BigDecimal bd2 = new BigDecimal(averageSubmit);  
			averageSubmit = bd2.setScale(2, RoundingMode.HALF_UP).doubleValue();
			outPationWritable.setAverageSubmit(averageSubmit);
			
			Double averageSubmitRate=totalSubmit/totalCost;    
			BigDecimal bd3 = new BigDecimal(averageSubmitRate);  
			averageSubmitRate = bd3.setScale(5, RoundingMode.HALF_UP).doubleValue();
			outPationWritable.setAverageSubmitRate(averageSubmitRate);
			
			Double cureRate=curePeople*1.0/outPationPeople;
			BigDecimal bd4 = new BigDecimal(cureRate);   
			cureRate = bd4.setScale(5, RoundingMode.HALF_UP).doubleValue();
			outPationWritable.setCureRate(cureRate);
			
			outPationWritable.setHospitalId(key.toString());
			contex.write(outPationWritable,NullWritable.get());
		}
	}

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/smart_guide", "root", "123456");
			
			Job job = Job.getInstance(conf, "OutPationMR");
			job.setJarByClass(OutPationMR.class);

			job.setMapperClass(OutpationMapper.class);
			job.setReducerClass(OutpationReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(OutPationWritable.class);

			Path inputPath = new Path("/SmartGuide-Output");
//			Path outPutPath = new Path("/Model3_3-Output");
			
//			FileSystem.get(conf).delete(outPutPath,true);
			
			FileInputFormat.addInputPath(job, inputPath);
//			FileOutputFormat.setOutputPath(job, outPutPath);

			job.setOutputFormatClass(DBOutputFormat.class);
			DBOutputFormat.setOutput(job, "outpation_info","hospitalId","totalPeople","averageCost","averageSubmit","averageSubmitRate","cureRate");
			
			job.addFileToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
