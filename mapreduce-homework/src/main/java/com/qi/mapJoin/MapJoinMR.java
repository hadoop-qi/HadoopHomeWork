package com.qi.mapJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.Emp;

public class MapJoinMR{
	
	public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		private HashMap<Integer, Emp> map=new HashMap<>();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String path=context.getCacheFiles()[0].toString();
			try(FileReader fileReader=new FileReader(path.substring(1));
					BufferedReader bufferedReader=new BufferedReader(fileReader);) {
				String content;
				while ((content=bufferedReader.readLine())!=null) {
					String[] line=content.split("\\s+");
					Emp emp=new Emp();
					emp.setId(Integer.parseInt(line[0]));
					emp.setName(line[1]);
					emp.setNum(line[2]);
					map.put(emp.getId(), emp);
				}
			} 
			
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line[]=value.toString().split("\\s+");
			int depId=Integer.parseInt(line[2]);
			String empInfo=map.get(depId).toString();
			context.write(new Text(value.toString()+"\t"+empInfo), NullWritable.get());
		
		}
		
	}
	
	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "MapJoin");
			job.setJarByClass(MapJoinMR.class);

			job.setMapperClass(MapJoinMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			
			Path inputPath = new Path("/joinInput/dep.txt");

			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/mapjoin-Output");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);

			// 可以添加任意个
			job.addCacheFile(new Path("/emp.txt").toUri());
			
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
