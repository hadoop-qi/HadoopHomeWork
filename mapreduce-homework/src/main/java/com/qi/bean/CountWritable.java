package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountWritable implements Writable{
	
	private String fileName;
	
	private Integer num;
	
	

	public CountWritable() {
		super();
	}
	

	public CountWritable(String fileName, Integer num) {
		super();
		this.fileName = fileName;
		this.num = num;
	}

	

	@Override
	public String toString() {
		return fileName+"---"+num;
	}

	

	public String getFileName() {
		return fileName;
	}


	public void setFileName(String fileName) {
		this.fileName = fileName;
	}


	public Integer getNum() {
		return num;
	}


	public void setNum(Integer num) {
		this.num = num;
	}


	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(fileName);
		out.writeInt(num);
	}

	public void readFields(DataInput in) throws IOException {

		fileName=in.readUTF();
		num=in.readInt();
	}

}
