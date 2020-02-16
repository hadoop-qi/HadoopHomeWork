package com.qi.model3_4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DiseaseWritable implements Writable,DBWritable{

	private String diseaseId;
	
	private String hospitalId;
	
	private Long treatCount;
	
	private Double cureRate;
	
	
	
	public DiseaseWritable() {
		super();
	}

	public DiseaseWritable(String diseaseId, String hospitalId, Long treatCount, Double cureRate) {
		super();
		this.diseaseId = diseaseId;
		this.hospitalId = hospitalId;
		this.treatCount = treatCount;
		this.cureRate = cureRate;
	}


	@Override
	public String toString() {
		return "diseaseName=" + diseaseId + ", hospitalId=" + hospitalId + ", treatCount="
				+ treatCount + ", cureRate=" + cureRate;
	}
	
	

	public String getDiseaseId() {
		return diseaseId;
	}

	public void setDiseaseId(String diseaseId) {
		this.diseaseId = diseaseId;
	}

	public String getHospitalId() {
		return hospitalId;
	}

	public void setHospitalId(String hospitalId) {
		this.hospitalId = hospitalId;
	}

	public Long getTreatCount() {
		return treatCount;
	}

	public void setTreatCount(Long treatCount) {
		this.treatCount = treatCount;
	}

	public Double getCureRate() {
		return cureRate;
	}

	public void setCureRate(Double cureRate) {
		this.cureRate = cureRate;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(diseaseId);
		out.writeUTF(hospitalId);
		out.writeLong(treatCount);
		out.writeDouble(cureRate);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		diseaseId=in.readUTF();
		hospitalId=in.readUTF();
		treatCount=in.readLong();
		cureRate=in.readDouble();
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {

		statement.setString(1, diseaseId);
		statement.setString(2, hospitalId);
		statement.setLong(3, treatCount);
		statement.setDouble(4, cureRate);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {

		diseaseId=resultSet.getString(1);
		hospitalId=resultSet.getString(2);
		treatCount=resultSet.getLong(3);
		cureRate=resultSet.getDouble(4);
	}

}
