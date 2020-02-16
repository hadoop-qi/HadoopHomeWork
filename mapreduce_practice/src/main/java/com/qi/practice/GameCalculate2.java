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

/**
 * @author Administrator
 *	2. 名字 胜率（小数点后两位） 使用场数
	3. 按照胜率降序排列，如果胜率相同，按照场数升序排序
 */
public class GameCalculate2 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, ResultWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ResultWritable>.Context context)
				throws IOException, InterruptedException {

			String[] strArray=value.toString().split(",");
			ResultWritable rw=new ResultWritable();
			rw.setCount(1);
			rw.setSucc(Integer.parseInt(strArray[1]));
			context.write(new Text(strArray[0]), rw);
			
		}

	}
	
	//map的输出是combiner的输入输出
public static class MyCombiner extends Reducer<Text, ResultWritable,HeroInfo, NullWritable> {

		
		@Override
		protected void reduce(Text key, Iterable<ResultWritable> value,
				Reducer<Text, ResultWritable,  HeroInfo,NullWritable>.Context contex) throws IOException, InterruptedException {

			Long count=0L;
			Long success=0L;
			for (ResultWritable rw : value) {
				count+=rw.getCount();
				success+=rw.getSucc();
			}
			double rate=success*1.0/count;
			BigDecimal bd = new BigDecimal(rate);  
			double f1 = bd.setScale(2, RoundingMode.HALF_UP).doubleValue();  
			HeroInfo hf=new HeroInfo();
			hf.setCount(count);
			hf.setRate(f1);
			hf.setHeroName(key.toString());
			contex.write(hf,NullWritable.get());
		}

	}


	public static class MyReducer extends Reducer<HeroInfo, NullWritable, NullWritable, HeroInfo> {

		
		@Override
		protected void reduce(HeroInfo key, Iterable<NullWritable> value,
				Reducer<HeroInfo, NullWritable, NullWritable, HeroInfo>.Context contex) throws IOException, InterruptedException {

			contex.write( NullWritable.get(), key);
		}

	}

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "GameCalculate");
		
		job.setJarByClass(GameCalculate2.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ResultWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(HeroInfo.class);
		
		Path inputPath = new Path("hdfs://master:9000/input/game.log");
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
