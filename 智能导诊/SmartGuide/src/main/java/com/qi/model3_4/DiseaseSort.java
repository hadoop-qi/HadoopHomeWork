package com.qi.model3_4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DiseaseSort extends WritableComparator{

	public DiseaseSort() {
		super(Text.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Text akey=(Text)a;     
		Text bkey=(Text)b;
		
		String[] akeyArray =akey.toString().split("\\s+");
		String[] bkeyArray =bkey.toString().split("\\s+");
		Double acureRate=Double.parseDouble(akeyArray[3]);
		Double bcureRate=Double.parseDouble(bkeyArray[3]);
		
		Long atreatCount=Long.parseLong(akeyArray[2]);
		Long btreatCount=Long.parseLong(bkeyArray[2]);
		
		String adisease=akeyArray[0];
		String bdisease=bkeyArray[0];
		
		if (adisease.compareTo(bdisease)!=0) {
			return adisease.compareTo(bdisease);
		}else if (acureRate.compareTo(bcureRate)!=0) {
			return -acureRate.compareTo(bcureRate);
		}else {
			return -atreatCount.compareTo(btreatCount);
		}
		
	}
	

}
