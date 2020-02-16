package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HeroInfo implements WritableComparable<HeroInfo>{

	
	private String heroName;
	
	private Double rate;
	
	private Long count;
	
	
	public HeroInfo() {
		super();
	}

	
	public HeroInfo(String heroName, Double rate, Long count, Long success) {
		super();
		this.heroName = heroName;
		this.rate = rate;
		this.count = count;
		
	}


	
	@Override
	public String toString() {
		return heroName+" "+rate+" "+count;
	}


	
	
	public String getHeroName() {
		return heroName;
	}


	public void setHeroName(String heroName) {
		this.heroName = heroName;
	}


	public Double getRate() {
		return rate;
	}


	public void setRate(Double rate) {
		this.rate = rate;
	}


	public Long getCount() {
		return count;
	}


	public void setCount(Long count) {
		this.count = count;
	}


	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(heroName);
		out.writeDouble(rate);
		out.writeLong(count);
		
		
	}

	public void readFields(DataInput in) throws IOException {
		
		heroName=in.readUTF();
		rate=in.readDouble();
		count=in.readLong();
		
	}

	public int compareTo(HeroInfo o) {
		
		
		if (rate.compareTo(o.getRate())>0) {
			
			return -1;
		}else if (rate.compareTo(o.getRate())==0) {
			
			return count.compareTo(o.getCount());
		}else {
			return 1;
		}
		
	}

}
