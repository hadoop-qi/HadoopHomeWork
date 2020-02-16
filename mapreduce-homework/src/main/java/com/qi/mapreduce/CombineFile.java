package com.qi.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.qi.bean.CountWritable;


public class CombineFile {


	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job = Job.getInstance(conf, "WordCount");
			job.setJarByClass(CombineFile.class);
			
			job.setMapperClass(CombineFileMapper.class);
			job.setReducerClass(CombineFileReducer.class);
			// reduce 数量为 0，就会把 map 的输出给放到 hdfs 上
			job.setNumReduceTasks(1);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path("/WordCound-output"));
			Path outputDir = new Path("/CombineFile-output");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);
			
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

	public static class CombineFileMapper extends Mapper<Text, CountWritable, Text, Text> {


		@Override
		protected void map(Text key, CountWritable value, Mapper<Text, CountWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, new Text(value.toString()));
		}
		
	}
	
	
	public static class CombineFileReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context contex) throws IOException, InterruptedException {
			
			
			TreeMap<Integer, String> map = new TreeMap<>(new Comparator<Integer>(
					) {

						@Override
						public int compare(Integer o1,Integer o2) {
							return -o1.compareTo(o2);
						}
			});
			
	
			for (Text value : values) {
				String[] line=value.toString().split("---");
				map.put(Integer.parseInt(line[1]), line[0]);
			}
			
			StringBuffer sb=new StringBuffer();
			
			for (Integer i : map.keySet()) {
				sb.append(map.get(i)+"---"+i);
				sb.append("||");
			}
		
			contex.write(key, new Text(sb.toString().substring(0,sb.length()-2)));
		}

	}
}
