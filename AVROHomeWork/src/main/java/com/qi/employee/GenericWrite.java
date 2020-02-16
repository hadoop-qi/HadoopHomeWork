package com.qi.employee;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class GenericWrite {

	
	public static void writeUser() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/avsc/employee.avsc"));
		GenericRecord employee = new GenericData.Record(schema);
		employee.put("name", "Ben");
		employee.put("gender", true);
		employee.put("salary", 10000.0);

		File file = new File("src/main/resources/avro/employee.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter);

		writer.create(schema, file);
		writer.append(employee);
		writer.close();
	}
	
	public static void main(String[] args) throws IOException {
		writeUser();
	}
}
