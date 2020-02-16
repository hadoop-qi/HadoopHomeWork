package com.qi.specific_mapping;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.qi.avrobean.User;

public class SpecificMapping {

	public static void writeUser() throws IOException {
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		User user2 = new User("Bean", 7, "red");
		User user3 = User.newBuilder().setName("Braney").setFavoriteColor("blue").setFavoriteNumber(null).build();

		DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(writer);

		dataFileWriter.create(user1.getSchema(), new File("users.avro"));
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);

		dataFileWriter.close();
	}

	private static void readUser() throws IOException {

		DatumReader<User> reader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> fileReader = new DataFileReader<User>(new File("users.avro"), reader);

		User user = null;
		while (fileReader.hasNext()) {
			// 复用user对象，避免重复分配内存和GC
			user = fileReader.next(user);
			System.out.println(user);
		}
		fileReader.close();
	}

	public static void main(String[] args) throws IOException {
//		writeUser();
		readUser();
	}
}
