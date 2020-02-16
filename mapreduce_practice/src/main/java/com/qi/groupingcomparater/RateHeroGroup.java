package com.qi.groupingcomparater;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RateHeroGroup extends WritableComparator{

	
	public RateHeroGroup() {
		super(Text.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Text akey=(Text)a;     
		Text bkey=(Text)b;
		String aKeyValue=akey.toString().substring(0,4);
		String bKeyValue=bkey.toString().substring(0,4);
		return -aKeyValue.compareTo(bKeyValue);
	}

}
