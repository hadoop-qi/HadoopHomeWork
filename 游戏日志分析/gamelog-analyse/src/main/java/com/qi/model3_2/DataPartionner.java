package com.qi.model3_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class DataPartionner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		
		String[] line=value.toString().split("\\s+");
		String startTime=line[3];
		String[] startTimeArray=startTime.split("-");
		String day=startTimeArray[2];
		int daynum=Integer.parseInt(day.substring(1,2));
		return daynum%numPartitions;
	}

//	@Override
//	public int getPartition(HeroInfo key, NullWritable value, int numPartitions) {
//		
//		if (key.getRate()>0.45 && key.getRate()<0.50) {
//			return 0 % numPartitions;
//		}
//		if (key.getRate()>=0.50 && key.getRate()<0.52) {
//			return 1 % numPartitions;
//		}
//		if (key.getRate()>=0.52) {
//			return 2 % numPartitions;
//		}
//		return 0;
//	}
	
	

}
