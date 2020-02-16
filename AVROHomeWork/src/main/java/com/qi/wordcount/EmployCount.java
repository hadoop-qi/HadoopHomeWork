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

import com.qi.avrobean.Employee2;
import com.qi.avrobean.EmployeeFile;
import com.qi.avrobean.SmallFile;
import com.qi.avrobean.WordCountResult;

public class EmployCount {
	
	public static class WordCountMapper extends Mapper<AvroKey<EmployeeFile>, NullWritable, AvroKey<Employee2>, NullWritable>{
		
		private static AvroKey<Employee2> outputKey=new AvroKey<>();
		
		@Override
		protected void map(AvroKey<EmployeeFile> key, NullWritable value, Mapper<AvroKey<EmployeeFile>, NullWritable, AvroKey<Employee2>, NullWritable>.Context context)
				throws IOException, InterruptedException {
			

			EmployeeFile employeeFile = key.datum();
			
			String content = employeeFile.getContent().toString();
			
			String[] lines = content.split("\\n");
			
			for (String row : lines) {
				Employee2 employee2=new Employee2();
				String[] words = row.split("\\s+");
					
				employee2.setNum(Integer.parseInt(words[0]));
				employee2.setName(words[1]);
				employee2.setAge(Integer.parseInt(words[2]));
				employee2.setDepartId(Integer.parseInt(words[3]));
				outputKey.datum(employee2);
				context.write(outputKey, NullWritable.get());
				
			}
		
		}
		
		
	}
		

	public static void main(String[] args) throws IOException {
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job=Job.getInstance(conf, "employFile");
		
		job.setJarByClass(EmployCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setInputKeySchema(job, EmployeeFile.getClassSchema());
		AvroJob.setOutputKeySchema(job, Employee2.getClassSchema());
		
		Path inputPath=new Path("/employFile-input");
		Path outPutPath=new Path("/employFile-output");
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
