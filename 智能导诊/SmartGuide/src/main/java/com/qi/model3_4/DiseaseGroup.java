package com.qi.model3_4;

import java.awt.RenderingHints.Key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DiseaseGroup extends WritableComparator{

	public DiseaseGroup() {
		super(Text.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Text akey=(Text)a;     
		Text bkey=(Text)b;
		
		String[] akeyArray =akey.toString().split("\\s+");
		String[] bkeyArray =bkey.toString().split("\\s+");
		
		return akeyArray[0].compareTo(bkeyArray[0]);
	}
	

}
