package com.qi.totalsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalSortMR {
	
	public static enum WordCount {
		Line,words;
	}
	
	public static class MyMApper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.getCounter(WordCount.words).increment(1L);
			context.write(key, value);
			
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, new Text());
		}
		
	}
	
	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");

			// feq 数字被选中的几率
			// numSamples 选中几个数字
			// 如果所有的数据都过了一遍，而样本没有获取够，就不再获取，分区会产生异常
			InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.2, 5);
			
			// 采样结果需要保存在 hdfs 上
			Path totalOrderPath = new Path("/total-order-partitioner");
			TotalOrderPartitioner.setPartitionFile(conf, totalOrderPath);

			Job job = Job.getInstance(conf, "total sort");
			job.setJarByClass(TotalSortMR.class);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(MyMApper.class);
			job.setReducerClass(MyReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			Path inputPath = new Path("/input/number.log");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/number-total-sort");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);

			job.setNumReduceTasks(5);

			// 使用全排序分区
			job.setPartitionerClass(TotalOrderPartitioner.class);
			
			// 把采样文件添加到分布式缓存中
			// 提交任务的时候，会自动从 hdfs 把分区文件下载到本地
			// TotalOrderPartitioner 要求这样做
			job.addCacheFile(totalOrderPath.toUri());

			// 绑定采样器和 job
			InputSampler.writePartitionFile(job, sampler);

			boolean flag = job.waitForCompletion(true);

			System.exit(flag ? 0 : 1);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

}
