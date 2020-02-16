package com.qi.merge;

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
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/small.avsc"));
		GenericRecord file1 = new GenericData.Record(schema);
		file1.put("name", "aaa");
		file1.put("content", AvroMergeSmallFile.result1);

		GenericRecord file2 = new GenericData.Record(schema);
		file2.put("name", "bbb");
		file2.put("content", AvroMergeSmallFile.result2);

		File file = new File("smallFile.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter);

		writer.create(schema, file);
		writer.append(file1);
		writer.append(file2);
		writer.close();
	}

	public static void readUser() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/small.avsc"));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		File file = new File("smallFile.avro");
		DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(file, datumReader);
		GenericRecord smallFile = null;
   
		while (reader.hasNext()) {
			smallFile = reader.next(smallFile);
			System.out.println(smallFile);
		}
		reader.close();
	}

	public static void main(String[] args) throws IOException {

//	AvroMergeSmallFile.main(args);
//		writeUser();
		readUser();
	}
}
