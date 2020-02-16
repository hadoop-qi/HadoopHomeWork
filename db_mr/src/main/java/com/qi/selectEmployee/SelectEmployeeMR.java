package com.qi.selectEmployee;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelectEmployeeMR {

	public static class SelectEmployeeMApper extends Mapper<LongWritable, Employee, NullWritable, Employee> {

		@Override
		protected void map(LongWritable key, Employee value, Mapper<LongWritable, Employee, NullWritable, Employee>.Context context)
				throws IOException, InterruptedException {

			System.out.println("=========="+value.toString()+"=============");
			context.write(NullWritable.get(),value);
 
		}
		
	}

	public static class SelectEmployeeReducer extends Reducer<NullWritable,Employee,NullWritable,Employee> {

		@Override
		protected void reduce(NullWritable key, Iterable<Employee> values,
				Reducer<NullWritable,Employee, NullWritable,Employee>.Context contex)
				throws IOException, InterruptedException {
			
			for (Employee employee : values) {
				System.out.println("=========="+employee.toString()+"=============");
				contex.write(NullWritable.get(),employee);
			}
		}

	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_db", "root", "123456");
			
			Job job = Job.getInstance(conf, "DBMR");
			job.setJarByClass(SelectEmployeeMR.class);
			
			job.setMapperClass(SelectEmployeeMApper.class);
			job.setReducerClass(SelectEmployeeReducer.class);
			         
			job.setOutputKeyClass(NullWritable.class); 
			job.setOutputValueClass(Employee.class);
			
			job.setInputFormatClass(DBInputFormat.class);
			DBInputFormat.setInput(job, Employee.class, "employee", "num!=2", "age desc","id","num","name","age","departId");
			
			Path outPutPath=new Path("/SelectEmployee-output");
			FileSystem.get(conf).delete(outPutPath,true);
			FileOutputFormat.setOutputPath(job, outPutPath);	
			
			job.addFileToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));
			System.exit(job.waitForCompletion(true)? 0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
