package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Emp implements Writable{

	private Integer id;
	
	private String name;
	
	private String num;
	
	
	
	public Emp() {
		super();
	}
	

	public Emp(Integer id, String name, String num) {
		super();
		this.id = id;
		this.name = name;
		this.num = num;
	}

	

	@Override
	public String toString() {
		return  name+"\t"+num;
	}

	

	public Integer getId() {
		return id;
	}


	public void setId(Integer id) {
		this.id = id;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getNum() {
		return num;
	}


	public void setNum(String num) {
		this.num = num;
	}


	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(id);
		out.writeUTF(name);
		out.writeUTF(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		id=in.readInt();
		name=in.readUTF();
		num=in.readUTF();
	}
	
	

}
