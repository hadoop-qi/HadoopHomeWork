package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ResultWritable implements Writable {

	private int succ;
	private int count;
	
	public ResultWritable() {
		super();
	}

	
	public ResultWritable(int succ, int count) {
		super();
		this.succ = succ;
		this.count = count;
	}


	

	public int getSucc() {
		return succ;
	}

	public void setSucc(int succ) {
		this.succ = succ;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}


	public void write(DataOutput out) throws IOException {
		out.writeInt(succ);
		out.writeInt(count);
		
	}


	public void readFields(DataInput in) throws IOException {

		succ=in.readInt();
		count=in.readInt();
	}
}
