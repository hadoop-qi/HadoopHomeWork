package com.qi.login_date;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserDate {
	
	// 需要计算的某一天的登录人数
	private static int oneDay;

	public static void main(String[] args) {
		try {

			// 执行 jar 的时候设置的参数，通过 args 传入到程序中
			// 多个参数之间以 空格 分隔
			// eg：java xxxx.class aaa bb cc
			//    args = {"aaa", "bb", "cc"}
			// eg：hadoop jar xxx.jar 777 888
			//    args = {"777", "888"}
			oneDay = Integer.parseInt(args[0]);
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "UserDate");
			job.setJarByClass(UserDate.class);
			
			// 设置 map 和 reduce 的类型
			job.setMapperClass(UserDateMapper.class);
			job.setReducerClass(UserDateReducer.class);

			// 设置 reduce 输出的 kv 对类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// 设置需要计算的数据的保存路径
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			Path inputPath = new Path("/game/user-login");
			FileInputFormat.addInputPath(job, inputPath);


			// 设置计算结果保存的文件夹，一定确保文件夹不存在
			Path outputDir = new Path("/game/2017-1-" + oneDay);
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
	
	public static class UserDateMapper extends Mapper<Text, Text, Text, IntWritable> {

		//1为int型 1<<2=000000(28个0)...100
		private int day = 1 << (oneDay - 1);
				
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int flag = Integer.parseInt(value.toString());
			
			// 判断是否有权限（在某天登录了）
			//flag可能为111,101,110,100与day作&运算都为100
			if ((flag & day) == day) {
				
				outputValue.set(flag);
				context.write(key, outputValue);
			}
		}
	}

	public static class UserDateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private int day = 1 << (oneDay - 1);

		// 总人数
		private int totoalUser;
		private int newUser;
		private int oldUser;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			for (IntWritable i : value) {
				
				// 全部登录数据
				int flag = i.get();
				
				// 截止到某天的连续登录数据
				//Integer.MAX_VALUE=111111111111(除了最高位符号位后面的31个1)
				//当oneDay=3时(Integer.MAX_VALUE >> (31 - oneDay))结果为:0000000...(28个0)..111
				int totalDay = flag & (Integer.MAX_VALUE >> (31 - oneDay));
				
				if (totalDay == day) {
					
					// 今天才登录
					newUser++;
				}else {
					
					// 以前登录过
					oldUser++;
				}
				
				totoalUser++;
			}
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(new Text("登录新人数："), new IntWritable(newUser));
			context.write(new Text("登录老人数："), new IntWritable(oldUser));
			context.write(new Text("登录总人数："), new IntWritable(totoalUser));
		}
	}
}
