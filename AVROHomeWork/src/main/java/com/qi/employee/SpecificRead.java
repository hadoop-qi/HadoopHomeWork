package com.qi.employee;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.qi.avrobean.Employee;

public class SpecificRead {

	private static void readUser() throws IOException {

		DatumReader<Employee> reader = new SpecificDatumReader<Employee>(Employee.class);
		DataFileReader<Employee> fileReader = new DataFileReader<Employee>(new File("src/main/resources/avro/employee.avro"), reader);

		Employee employee = null;
		while (fileReader.hasNext()) {
			// 复用user对象，避免重复分配内存和GC
			employee = fileReader.next(employee);
			System.out.println(employee);
		}
		fileReader.close();
	}
	
	public static void main(String[] args) throws IOException {
		readUser();
	}
}
