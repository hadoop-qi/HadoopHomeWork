package com.zhiyou100.basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) {
		
		try {
			
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf, "wordcount");
			job.setJarByClass(WordCount.class);
			
			// 设置使用的 map 和 reduce 的类
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			// 设置 map 输出的 kv 对类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			// 设置 reduce 输出的 kv 对类型
			// 如果 map 和 reduce 的输出 kv 类型一致，可以不设置 map 的输出类型
			// 如果不一样，必须分别设置
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			// 设置需要计算的数据的保存路径
			Path inputPath = new Path("hdfs://master:9000/fffe.log");
			FileInputFormat.addInputPath(job, inputPath);

			// 设置计算结果保存的文件夹，一定确保文件夹不存在
			Path outputDir = new Path("hdfs://master:9000/feee-word-count333");
			FileOutputFormat.setOutputPath(job, outputDir);
			
			// 提交任务并等待完成，返回值表示任务执行结果
			boolean flag = job.waitForCompletion(true);
			
			// 如果执行成功，退出程序
			System.exit(flag ? 0 : 1);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text outputKey = new Text();
		
		// 对于至不会发生变化的 outputKey 或 outputValue 我们使用 static
		// 修饰可以提高代码执行的效率
		private static IntWritable outputValue = new IntWritable(1);
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			// value 是每一行文字，需要分割出每一个单词
			// 取出每一行内容
			String row = value.toString();
			
			// 按照空白字符进行分割
			String[] words = row.split("\\s+");
			
			// 以单词为 key， 1 为 value 进行输出
			for (String word : words) {
				
				// 为 outputKey 设置值
				outputKey.set(word);
				
				// 输出 kv 对
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			// 统计循环次数，即单词出现的次数
			int sum = 0;
			
			for (IntWritable one : value) {
				
				sum += 1;
			}
		
			// 以单词作为 key
			outputKey = key;
			
			// 以次数作为 value
			outputValue.set(sum);
			
			// 输出 kv 对
			context.write(outputKey, outputValue);
		}
	}
}





