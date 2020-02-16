package com.qi.readAvroToDB;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class Employee implements WritableComparable<Employee>,DBWritable{

//	private Integer id;
	
	private Integer num;
	
	private String name;
	
	private Integer age;
	
	private Integer departId;
	
	
	
	public Employee() {
		super();
	}
	
	

	public Employee(Integer id, Integer num, String name, Integer age, Integer departId) {
		super();
		
		this.num = num;
		this.name = name;
		this.age = age;
		this.departId = departId;
	}



	@Override
	public String toString() {
		return "Employee [num=" + num + ", name=" + name + ", age=" + age + ", departId=" + departId
				+ "]";
	}



	public Integer getNum() {
		return num;
	}



	public void setNum(Integer num) {
		this.num = num;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}



	public Integer getAge() {
		return age;
	}



	public void setAge(Integer age) {
		this.age = age;
	}



	public Integer getDepartId() {
		return departId;
	}



	public void setDepartId(Integer departId) {
		this.departId = departId;
	}



	@Override
	public void write(DataOutput out) throws IOException {

//		out.writeInt(id);
		out.writeInt(num);
		out.writeUTF(name);
		out.writeInt(age);
		out.writeInt(departId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

//		id=in.readInt();
		num=in.readInt();
		name=in.readUTF();
		age=in.readInt();
		departId=in.readInt();
	}

	@Override
	public int compareTo(Employee o) {
		
		return this.num.compareTo(o.getNum());
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		
//		statement.setInt(1, id);
		statement.setInt(1, num);
		statement.setString(2, name);
		statement.setInt(3, age);
		statement.setInt(4, departId);
		
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {

//		id=resultSet.getInt(1);
		num=resultSet.getInt(1);
		name=resultSet.getString(2);
		age=resultSet.getInt(3);
		departId=resultSet.getInt(4);
	}

}
