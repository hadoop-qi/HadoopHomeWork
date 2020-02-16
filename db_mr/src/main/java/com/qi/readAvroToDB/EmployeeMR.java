package com.qi.readAvroToDB;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

public class EmployeeMR {

	public static class DBMApper extends Mapper<AvroKey<Employee2>, NullWritable, Employee, NullWritable> {

		@Override
		protected void map(AvroKey<Employee2> key, NullWritable value, Mapper<AvroKey<Employee2>, NullWritable, Employee, NullWritable>.Context context)
				throws IOException, InterruptedException {

			Employee2 employee2=key.datum();
			Employee employee=new Employee();
			employee.setAge(employee2.getAge());
			employee.setDepartId(employee2.getDepartId());
			employee.setNum(employee2.getNum());
			employee.setName(employee2.getName().toString());
			context.write(employee, NullWritable.get());

		}

	}

	public static class DBReducer extends Reducer<Employee, NullWritable, Employee, NullWritable> {

		@Override
		protected void reduce(Employee key, Iterable<NullWritable> values,
				Reducer<Employee, NullWritable, Employee, NullWritable>.Context contex)
				throws IOException, InterruptedException {

			contex.write(key, NullWritable.get());
		}

	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_db", "root", "123456");
			
			Job job = Job.getInstance(conf, "EmployeeMR");
			job.setJarByClass(EmployeeMR.class);
			
			job.setMapperClass(DBMApper.class);
			job.setReducerClass(DBReducer.class);
			
			job.setOutputKeyClass(Employee.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(AvroKeyInputFormat.class);
			AvroJob.setInputKeySchema(job, Employee2.getClassSchema());
			
			Path inputPath=new Path("/employFile-output");
			AvroKeyInputFormat.addInputPath(job, inputPath);
			
			job.setOutputFormatClass(DBOutputFormat.class);
			DBOutputFormat.setOutput(job, "employee","num","name","age","departId");
			
			job.addFileToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));
			System.exit(job.waitForCompletion(true)? 0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
