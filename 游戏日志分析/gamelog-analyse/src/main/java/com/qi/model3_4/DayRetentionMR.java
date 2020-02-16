package com.qi.model3_4;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.model3_1.GameMR;
import com.qi.model3_1.GameMR.GameMapper;
import com.qi.model3_1.GameMR.GameReducer;

public class DayRetentionMR {
	
	private static ArrayList<String> totalAliveTwo=new ArrayList<>();
	private static ArrayList<String> totalAlivethree=new ArrayList<>();
	private static ArrayList<String> totalAliveSeven=new ArrayList<>();
	
	public static class DayRetentionMapper extends Mapper<LongWritable, Text, Text, Text>{

		private String fileName = null;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();

			int index = path.lastIndexOf("/");

			fileName = path.substring(index + 1);
		}


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			if (fileName.equals("part-r-00000")||fileName.equals("part-r-00006")) {
				
				totalAliveTwo.add(value.toString());
				totalAlivethree.add(value.toString());
				totalAliveSeven.add(value.toString());
			} else if(fileName.equals("part-r-00005")){
				totalAlivethree.add(value.toString());
				totalAliveSeven.add(value.toString());
			}else {
				totalAliveSeven.add(value.toString());
			}
			
		}
		
	}
	
	public static class DayRetentionReducer extends Reducer<Text, Text, Text, NullWritable>{

		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String, Integer> map = new HashedMap();
			for (String deviceId : totalAliveTwo) {
			    if (map.get(deviceId) == null) {
			        map.put(deviceId, 1);
			    } else {
			        map.put(deviceId, map.get(deviceId) + 1);
			    }
			}
			
			int aliveTwoCount=0;
			for (String deviceId : map.keySet()) {
				if (map.get(deviceId)==2) {
					aliveTwoCount++;
				}
			}
			
			Double aliveTwoRate=aliveTwoCount*1.0/totalAliveTwo.size();
			BigDecimal bd = new BigDecimal(aliveTwoRate);  
			aliveTwoRate = bd.setScale(5, RoundingMode.HALF_UP).doubleValue();
			//=====================================================================================================
			
			Map<String, Integer> threemap = new HashedMap();
			for (String deviceId : totalAlivethree) {
			    if (threemap.get(deviceId) == null) {
			    	threemap.put(deviceId, 1);
			    } else {
			    	threemap.put(deviceId, threemap.get(deviceId) + 1);
			    }
			}
			
			int aliveThreeCount=0;
			for (String deviceId : threemap.keySet()) {
				if (threemap.get(deviceId)==3) {
					aliveThreeCount++;
				}
			}
			Double aliveThreeRate=aliveThreeCount*1.0/totalAlivethree.size();
			BigDecimal bd2 = new BigDecimal(aliveThreeRate);  
			aliveThreeRate = bd2.setScale(5, RoundingMode.HALF_UP).doubleValue();
			//============================================================================================================
			
			Map<String, Integer> sevenmap = new HashedMap();
			for (String deviceId : totalAliveSeven) {
			    if (sevenmap.get(deviceId) == null) {
			    	sevenmap.put(deviceId, 1);
			    } else {
			    	sevenmap.put(deviceId, sevenmap.get(deviceId) + 1);
			    }
			}
			
			int aliveSevenCount=0;
			for (String deviceId : sevenmap.keySet()) {
				if (sevenmap.get(deviceId)==7) {
					aliveSevenCount++;
				}
			}
			Double aliveSevenRate=aliveSevenCount*1.0/totalAliveSeven.size();
			BigDecimal bd3 = new BigDecimal(aliveSevenRate);  
			aliveSevenRate = bd3.setScale(5, RoundingMode.HALF_UP).doubleValue();
			
			context.write(new Text(aliveTwoRate+"\t"+aliveThreeRate+"\t"+aliveSevenRate), NullWritable.get());
			
		}
	}
	
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "DayRetentionMR");
			job.setJarByClass(DayRetentionMR.class);

			job.setMapperClass(DayRetentionMapper.class);
			job.setReducerClass(DayRetentionReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/DataCut-Output");
			Path outPutPath = new Path("/DayRetentionRate-output");

			FileInputFormat.addInputPath(job, inputPath);
			FileSystem.get(conf).delete(outPutPath, true);
			FileOutputFormat.setOutputPath(job, outPutPath);
		
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
