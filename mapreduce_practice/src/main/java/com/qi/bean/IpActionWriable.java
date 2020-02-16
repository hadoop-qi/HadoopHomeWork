package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IpActionWriable implements WritableComparable<IpActionWriable>{


	private String ip;
	
	private String action;

	public IpActionWriable() {
		super();
	}

	public IpActionWriable(String ip, String action) {
		super();
		this.ip = ip;
		this.action = action;
	}

	@Override
	public String toString() {
		return this.ip+"---"+this.action;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(action);
	}

	public void readFields(DataInput in) throws IOException {
		ip=in.readUTF();
		action=in.readUTF();
	}

	

	public int compareTo(IpActionWriable ipAction){
		
		
		return ip.compareTo(ipAction.getIp());
		
	}

	
}
