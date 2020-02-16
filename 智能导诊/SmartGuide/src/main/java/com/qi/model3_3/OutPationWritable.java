package com.qi.model3_3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class OutPationWritable implements Writable,DBWritable{

	private String hospitalId;
	
	private Long totalPeople;
	
	private Double averageCost;
	
	private Double averageSubmit;
	
	private Double averageSubmitRate;
	
	private Double cureRate;
	
	
	
	public OutPationWritable() {
		super();
	}
	

	public OutPationWritable(String hospitalId, Long totalPeople, Double averageCost, Double averageSubmit,
			Double averageSubmitRate, Double cureRate) {
		super();
		this.hospitalId = hospitalId;
		this.totalPeople = totalPeople;
		this.averageCost = averageCost;
		this.averageSubmit = averageSubmit;
		this.averageSubmitRate = averageSubmitRate;
		this.cureRate = cureRate;
	}


	@Override
	public String toString() {
		return "hospitalId="+hospitalId+",totalPeople=" + totalPeople + ", averageCost=" + averageCost + ", averageSubmit="
				+ averageSubmit + ", averageSubmitRate=" + averageSubmitRate+",cureRate="+cureRate;
	}

	
	

	public Long getTotalPeople() {
		return totalPeople;
	}



	public void setTotalPeople(Long totalPeople) {
		this.totalPeople = totalPeople;
	}



	public Double getAverageCost() {
		return averageCost;
	}



	public void setAverageCost(Double averageCost) {
		this.averageCost = averageCost;
	}



	public Double getAverageSubmit() {
		return averageSubmit;
	}



	public void setAverageSubmit(Double averageSubmit) {
		this.averageSubmit = averageSubmit;
	}



	public Double getAverageSubmitRate() {
		return averageSubmitRate;
	}



	public void setAverageSubmitRate(Double averageSubmitRate) {
		this.averageSubmitRate = averageSubmitRate;
	}



	public Double getCureRate() {
		return cureRate;
	}


	public void setCureRate(Double cureRate) {
		this.cureRate = cureRate;
	}

	

	public String getHospitalId() {
		return hospitalId;
	}


	public void setHospitalId(String hospitalId) {
		this.hospitalId = hospitalId;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(hospitalId);
		out.writeLong(totalPeople);
		out.writeDouble(averageCost);
		out.writeDouble(averageSubmit);
		out.writeDouble(averageSubmitRate);
		out.writeDouble(cureRate);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		hospitalId=in.readUTF();
		totalPeople=in.readLong();
		averageCost=in.readDouble();
		averageSubmit=in.readDouble();
		averageSubmitRate=in.readDouble();
		cureRate=in.readDouble();
	}


	@Override
	public void write(PreparedStatement statement) throws SQLException {

		statement.setString(1, hospitalId);
		statement.setLong(2, totalPeople);
		statement.setDouble(3, averageCost);
		statement.setDouble(4, averageSubmit);
		statement.setDouble(5, averageSubmitRate);
		statement.setDouble(6, cureRate);
	}


	@Override
	public void readFields(ResultSet resultSet) throws SQLException {

		hospitalId=resultSet.getString(1);
		totalPeople=resultSet.getLong(2);
		averageCost=resultSet.getDouble(3);
		averageSubmit=resultSet.getDouble(4);
		averageSubmitRate=resultSet.getDouble(5);
		cureRate=resultSet.getDouble(6);
		
	}

}
