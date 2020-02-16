package com.qi.generic_mapping;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericMapping {

	//相对路径是相对于AVROTest项目名
	public static void writeUser() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"));
		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Ben");
		user1.put("favorite_number", 256);

		GenericRecord user2 = new GenericData.Record(schema);
		user2.put("name", "Alyssa");
		user2.put("favorite_number", 7);
		user2.put("favorite_color", "red");

		File file = new File("users-generic.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter);

		writer.create(schema, file);
		writer.append(user1);
		writer.append(user2);
		writer.close();
	}

	public static void readUser() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/user.avsc"));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		File file = new File("users-generic.avro");
		DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(file, datumReader);
		GenericRecord user = null;

		while (reader.hasNext()) {
			user = reader.next(user);
			System.out.println(user);
		}
		reader.close();
	}

	public static void main(String[] args) throws IOException {

//		writeUser();
		readUser();
	}
}
