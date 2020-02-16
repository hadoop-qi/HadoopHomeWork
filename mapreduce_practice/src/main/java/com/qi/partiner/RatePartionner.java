package com.qi.partiner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.qi.bean.HeroInfo;

public class RatePartionner extends Partitioner<HeroInfo, NullWritable>{

	@Override
	public int getPartition(HeroInfo key, NullWritable value, int numPartitions) {
		
		if (key.getRate()>0.45 && key.getRate()<0.50) {
			return 0 % numPartitions;
		}
		if (key.getRate()>=0.50 && key.getRate()<0.52) {
			return 1 % numPartitions;
		}
		if (key.getRate()>=0.52) {
			return 2 % numPartitions;
		}
		return 0;
	}
	
	

}
