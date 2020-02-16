package com.qi.groupingcomparater;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RateHero {

	public static class RateHeroMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		
		private DoubleWritable outputKey=new DoubleWritable();
		private Text outputValue=new Text();
		
	}
}
