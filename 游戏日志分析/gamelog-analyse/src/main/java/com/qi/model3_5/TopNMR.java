package com.qi.model3_5;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeMap;
import java.util.TreeSet;

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

public class TopNMR {
	
	public static class TopNMapper extends Mapper<LongWritable, Text, Text, Text> {

		private static Text outputKey=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
			String[] line= value.toString().split("\\s+");
			String device=line[0];
			outputKey.set(device);
			context.write(outputKey, value);
		}
	}
	
	public static class TopNReducer extends Reducer<Text, Text, Text, NullWritable>{

		private static TreeMap<String, String> map=new TreeMap<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				String[] line1=o1.split("#");
				String[] line2=o2.split("#");
				Long duration1=Long.parseLong(line1[0]);
				Integer count1=Integer.parseInt(line1[1]);
				Long startTime1=Long.parseLong(line1[2]);
				
				Long duration2=Long.parseLong(line2[0]);
				Integer count2=Integer.parseInt(line2[1]);
				Long startTime2=Long.parseLong(line2[2]);
				
				if (duration1.compareTo(duration2)!=0) {
					return -duration1.compareTo(duration2);
				}else if (count1.compareTo(count2)!=0) {
					return -count1.compareTo(count2);
				}else {
					return startTime1.compareTo(startTime2);
				}
			}
		});
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
		
			int count=0;
			Long duration=0L;
			TreeSet<Long> set=new TreeSet<>();
			for (Text text : values) {
				count++;
				String[] line=text.toString().split("\\s+");
				duration+=Long.parseLong(line[5]);
				ZoneId zoneId = ZoneId.systemDefault();
		        LocalDateTime localDateTime = LocalDateTime.parse(line[3]);
		        ZonedDateTime zdt = localDateTime.atZone(zoneId);
		        Date date = Date.from(zdt.toInstant());
		        set.add(date.getTime());
			}
			Long startTime=set.first();
			
			String mapKey=duration+"#"+count+"#"+startTime;
			map.put(mapKey, key.toString());
			
			if (map.size()==21) {
				map.remove(map.lastKey());
			}
			
			
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
		
			for (String value : map.keySet()) {
				
				context.write(new Text(map.get(value)), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "TopNMR");
			job.setJarByClass(TopNMR.class);

			job.setMapperClass(TopNMapper.class);
			job.setReducerClass(TopNReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/GameInput");
			Path outPutPath = new Path("/TopN-Output");

			FileInputFormat.addInputPath(job, inputPath);
			FileSystem.get(conf).delete(outPutPath, true);
			FileOutputFormat.setOutputPath(job, outPutPath);
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
