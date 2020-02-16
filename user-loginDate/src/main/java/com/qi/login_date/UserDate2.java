package com.qi.login_date;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserDate2 {
	
	// 需要计算的某一天的登录人数

	public static void main(String[] args) {
		try {

			// 执行 jar 的时候设置的参数，通过 args 传入到程序中
			// 多个参数之间以 空格 分隔
			// eg：java xxxx.class aaa bb cc
			//    args = {"aaa", "bb", "cc"}
			// eg：hadoop jar xxx.jar 777 888
			//    args = {"777", "888"}
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			conf.set("oneDay", args[0]);

			Job job = Job.getInstance(conf, "UserDate2");
			job.setJarByClass(UserDate2.class);
			
			// 设置 map 和 reduce 的类型
			job.setMapperClass(UserDate2Mapper.class);
			job.setReducerClass(UserDate2Reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			// 设置需要计算的数据的保存路径
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			Path inputPath = new Path("/game/user-login");
			FileInputFormat.addInputPath(job, inputPath);


			// 设置计算结果保存的文件夹，一定确保文件夹不存在
			Path outputDir = new Path("/game/2017-1-" + args[0]);
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
	
	
	




	public static class UserDate2Mapper extends Mapper<Text, Text, Text, IntWritable> {
				
		private int oneDay;

		private IntWritable outputValue = new IntWritable();


		@Override
		protected void setup(Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			oneDay = Integer.parseInt(context.getConfiguration().get("oneDay"));
		}


		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			// 截取到某一天的登录数据
			//Integer.MAX_VALUE=111111111111(除了最高位符号位后面的31个1)
			//当oneDay=3时(Integer.MAX_VALUE >> (31 - oneDay))结果为:0000000...(28个0)..111
			int flag = Integer.parseInt(value.toString()) & (Integer.MAX_VALUE >> (31 - oneDay));
			
			// 不等于 0 说明在指定的时间段内登录过
			if (flag != 0) {
				
				outputValue.set(flag);
				context.write(key, outputValue);
			}
		}
	}

	public static class UserDate2Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

		private int oneDay;

		private int day;

		// 总人数
		private int allUser;
		private int totoalUser;
		private int newUser;
		private int oldUser;
		
		// 最近两天内连续登录的人数
		private int twoDayUser;
		
		// 最近两天内登录过的总人数
		private int allTwoDayUser;
		
		
		@Override
		protected void setup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			oneDay = Integer.parseInt(context.getConfiguration().get("oneDay"));

			day = 1 << (oneDay - 1);
		}

		@Override  
		protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (IntWritable i : value) {
				
				allUser++;

				int flag = i.get();
				
				if ((flag & day) == day) {
					
					// 在某一天登录的人
					totoalUser++;
					
					if (flag == day) {
						
						// 某一天的新用户
						newUser++;
					}else {
						
						// 某一天的老用户
						oldUser++;
					}
				}
				
				
				if (oneDay > 1) {
					
					// 最近两天都登录的最小值 = 只有最近两天登录
					int twoMin = 3 << (oneDay - 2);
					
					if (flag >= twoMin) {
						// 最近两天内连续登录的人
						twoDayUser++;
					}
					
					// 最近两天内登录的最小值
					int allTwoMin = 1 << (oneDay - 2);
					
					if (flag >= allTwoMin) {
						
						// 最近两天内登录过的人
						allTwoDayUser++;
					}
				}
				
			}
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(new Text(oneDay + " 号登录新人数：" + newUser), NullWritable.get());
			context.write(new Text(oneDay + " 号登录老人数：" + oldUser), NullWritable.get());
			context.write(new Text(oneDay + " 号登录总人数：" + totoalUser), NullWritable.get());
			context.write(new Text("截止到 " + oneDay + " 号登录总人数：" + allUser), NullWritable.get());
			
			if (oneDay > 1) {
				context.write(new Text("最近两天内连续登录的人数：" + twoDayUser), NullWritable.get());
				context.write(new Text("最近两天内登录过的总人数：" + allTwoDayUser), NullWritable.get());	
				context.write(new Text("次日留存率：" + (twoDayUser * 1.0 / allTwoDayUser)), NullWritable.get());	
			}
		}
	}
}
