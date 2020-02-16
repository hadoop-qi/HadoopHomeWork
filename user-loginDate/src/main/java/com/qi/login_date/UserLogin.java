package com.qi.login_date;

import java.io.IOException;
import java.time.LocalDateTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserLogin {

	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "UserLogin");
			job.setJarByClass(UserLogin.class);
			
			// 设置 map 和 reduce 的类型
			job.setMapperClass(UserLoginMapper.class);
			job.setReducerClass(UserLoginReducer.class);

			// 设置 reduce 输出的 kv 对类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// 设置需要计算的数据的保存路径
			Path inputPath = new Path("/GameInput/game-2018-01-01-2018-01-07.log");
			FileInputFormat.addInputPath(job, inputPath);


			// 设置计算结果保存的文件夹，一定确保文件夹不存在
			Path outputDir = new Path("/game/user-login");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);

			// 提交任务并等待完成，返回值表示任务执行结果
			boolean flag = job.waitForCompletion(true);

			// 如果执行成功，退出程序
			System.exit(flag ? 0 : 1);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
	
	public static class UserLoginMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] words = line.split("\t");
			
			outputKey.set(words[0]);
			outputValue.set(Integer.parseInt(words[3].substring(8, 10)));
			
			
			context.write(outputKey, outputValue);
		}
	}

	public static class UserLoginReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int flag = 0;
			
			for (IntWritable day : value) {
				
				// 把登录的那一天记录为 1
				//将一个设备一个月内每天的登录情况用二进制表示,并进行或运算
				flag |= (1 << (day.get() - 1));
			}
			
			outputValue.set(flag);
			
			context.write(key, outputValue);
		}
	}
}
