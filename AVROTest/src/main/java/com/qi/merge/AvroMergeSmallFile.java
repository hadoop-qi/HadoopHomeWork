package com.qi.merge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroMergeSmallFile {
	
	public static  String result1;
	public static  String result2;

	
	public static void main(String[] args) {
		
		String filePath1="E:/avro/aaa.txt";
		String filePath2="E:/avro/bbb.txt";
		
		try(FileReader fileReader=new FileReader(filePath1);
				BufferedReader bufferedReader=new BufferedReader(fileReader);
				FileReader fileReader2=new FileReader(filePath2);
				BufferedReader bufferedReader2=new BufferedReader(fileReader2);) {
			StringBuilder content1=new StringBuilder();
			String readText1=null;
			while ((readText1=bufferedReader.readLine())!=null) {
				content1.append(readText1).append("\n");
			}
			
			StringBuilder content2=new StringBuilder();
			String readText2=null;
			while ((readText2=bufferedReader2.readLine())!=null) {
				content2.append(readText2).append("\n");
			}
			
			result1=content1.toString();
			result2=content2.toString();
			System.out.println(content1.toString());
			System.out.println(content2.toString());
			
			SmallFile smallFile1=new SmallFile();
			smallFile1.setName("aaa");
			smallFile1.setContent(content1.toString());
			
			SmallFile smallFile2=new SmallFile();
			smallFile2.setName("bbb");
			smallFile2.setContent(content2.toString());
			
			DatumWriter<SmallFile> writer = new SpecificDatumWriter<SmallFile>(SmallFile.class);
			DataFileWriter<SmallFile> dataFileWriter = new DataFileWriter<SmallFile>(writer);

			dataFileWriter.create(smallFile1.getSchema(), new File("smallFile.avro"));
			dataFileWriter.append(smallFile1);
			dataFileWriter.append(smallFile2);
			dataFileWriter.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
