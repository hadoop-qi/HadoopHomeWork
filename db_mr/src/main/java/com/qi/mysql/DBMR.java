package com.qi.mysql;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

public class DBMR {

	public static class DBMApper extends Mapper<LongWritable, User, User, NullWritable> {

		@Override
		protected void map(LongWritable key, User value, Mapper<LongWritable, User, User, 			NullWritable>.Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());

		}

	}

	public static class DBReducer extends Reducer<User, NullWritable, User, NullWritable> {

		@Override
		protected void reduce(User key, Iterable<NullWritable> values,
				Reducer<User, NullWritable, User, NullWritable>.Context contex)
				throws IOException, InterruptedException {

			contex.write(key, NullWritable.get());
		}

	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", 			"jdbc:mysql://master:3306/hadoop_db", "root", "123456");
			
			Job job = Job.getInstance(conf, "DBMR");
			job.setJarByClass(DBMR.class);
			
			job.setMapperClass(DBMApper.class);
			job.setReducerClass(DBReducer.class);
			
			job.setOutputKeyClass(User.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(DBInputFormat.class);
			DBInputFormat.setInput(job, User.class, "user", null, "id", "id","name","age");
			
			job.setOutputFormatClass(DBOutputFormat.class);
			DBOutputFormat.setOutput(job, "user_back", "id","name","age");
			
			job.addFileToClassPath(new Path("/mysql-connector-java-5.1.38.jar"));
			System.exit(job.waitForCompletion(true)? 0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
