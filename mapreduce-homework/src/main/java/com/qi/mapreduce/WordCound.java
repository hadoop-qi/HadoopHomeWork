package com.qi.mapreduce;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.qi.bean.CountWritable;


public class WordCound {


	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job = Job.getInstance(conf, "WordCount");
			job.setJarByClass(WordCound.class);
			
			job.setMapperClass(WordCoundMapper .class);
			job.setReducerClass(WordCoundReducer.class);
			// reduce 数量为 0，就会把 map 的输出给放到 hdfs 上
			job.setNumReduceTasks(1);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CountWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			/*FileInputFormat.addInputPath(job, new Path("/input/apr License.txt"));
			FileInputFormat.addInputPath(job, new Path("/input/Changelog.txt"));
			FileInputFormat.addInputPath(job, new Path("/input/CyrusSASL License.txt"));
			FileInputFormat.addInputPath(job, new Path("/input/OpenSSL License.txt"));
			FileInputFormat.addInputPath(job, new Path("/input/Subversion license.txt"));*/
			FileInputFormat.addInputPath(job, new Path("/input"));
			
			
			Path outputDir = new Path("/WordCound-output");
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

	public static class WordCoundMapper extends Mapper<LongWritable, Text, Text, Text> {

		private String fileName;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			// setup 只会执行一次，map 会执行 n 多次
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			Path filePath = inputSplit.getPath();
			
			String path = filePath.toString();
//			String name = path.substring(25);
			
			int index = path.lastIndexOf("/");
			fileName = path.substring(index+1);
			
			
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] line=value.toString().split("\\s+|[\\pP‘’“”]");
			for (String string : line) {
				if (!("".equals(string))) {
				context.write(new Text(string+";"+fileName),new Text("1"));
				}
			}
			
		}
		
	}
	
	
	public static class WordCoundReducer extends Reducer<Text, Text, Text, CountWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, CountWritable>.Context contex) throws IOException, InterruptedException {
			
			int sum=0;
			for (Text value : values) {
					sum+=Integer.parseInt(value.toString());
			}
			CountWritable countWritable= new CountWritable();
			countWritable.setNum(sum);
			String[] strArray=key.toString().split(";");
			countWritable.setFileName(strArray[1]);
			contex.write(new Text(strArray[0]), countWritable);
			
		}

	}
}
