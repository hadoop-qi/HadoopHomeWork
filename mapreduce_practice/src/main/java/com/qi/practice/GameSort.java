package com.qi.practice;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.HeroInfo;
import com.qi.bean.ResultWritable;
import com.qi.partiner.RatePartionner;

/**
 * @author Administrator
 *	2. 名字 胜率（小数点后两位） 使用场数
	3. 按照胜率降序排列，如果胜率相同，按照场数升序排序
 */
public class GameSort {
	public static class MyMapper extends Mapper<LongWritable, Text,  HeroInfo,NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, HeroInfo,NullWritable>.Context context)
				throws IOException, InterruptedException {

			String[] strArray=value.toString().split("\\s+");
			HeroInfo hf=new HeroInfo();
			hf.setHeroName(strArray[0]);
			hf.setRate(Double.parseDouble(strArray[1]));
			hf.setCount(Long.parseLong(strArray[2]));
			context.write(hf,NullWritable.get());
			
		}

	}


	public static class MyReducer extends Reducer<HeroInfo, NullWritable, HeroInfo, NullWritable> {

		
		@Override
		protected void reduce(HeroInfo key, Iterable<NullWritable> value,
				Reducer<HeroInfo, NullWritable, HeroInfo, NullWritable>.Context contex) throws IOException, InterruptedException {

			for (NullWritable nullWritable : value) {
				contex.write( key, nullWritable);
			}
			
		}

	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "GameSort");

		job.setJarByClass(GameSort.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(HeroInfo.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(HeroInfo.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(3);
		
		job.setPartitionerClass(RatePartionner.class);
		
		Path inputPath = new Path("hdfs://master:9000/input/game_result1");
		Path outPutPath = new Path("hdfs://master:9000/output");
		FileSystem.get(conf).delete(outPutPath,true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPutPath);

		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
