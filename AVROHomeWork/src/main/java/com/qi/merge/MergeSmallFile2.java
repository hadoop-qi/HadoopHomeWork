package com.qi.merge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.qi.avrobean.EmployeeFile;
import com.qi.avrobean.SmallFile;

public class MergeSmallFile2 {
	
	
	public static void main(String[] args) {
		
		String filePath1="E:/avro/1.txt";
		String filePath2="E:/avro/2.txt";
		String filePath3="E:/avro/3.txt";
		
		try(FileReader fileReader=new FileReader(filePath1);
				BufferedReader bufferedReader=new BufferedReader(fileReader);
				FileReader fileReader2=new FileReader(filePath2);
				BufferedReader bufferedReader2=new BufferedReader(fileReader2);
				FileReader fileReader3=new FileReader(filePath3);
				BufferedReader bufferedReader3=new BufferedReader(fileReader3);) {
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
			
			StringBuilder content3=new StringBuilder();
			String readText3=null;
			while ((readText3=bufferedReader3.readLine())!=null) {
				content3.append(readText3).append("\n");
			}
			
		
//			System.out.println(content1.toString());
//			System.out.println(content2.toString());
			
			EmployeeFile employeeFile1=new EmployeeFile();
			employeeFile1.setName("1");
			employeeFile1.setContent(content1.toString());
			
			EmployeeFile employeeFile2=new EmployeeFile();
			employeeFile2.setName("2");
			employeeFile2.setContent(content2.toString());
			
			EmployeeFile employeeFile3=new EmployeeFile();
			employeeFile3.setName("3");
			employeeFile3.setContent(content3.toString());
			
			DatumWriter<EmployeeFile> writer = new SpecificDatumWriter<EmployeeFile>(EmployeeFile.class);
			DataFileWriter<EmployeeFile> dataFileWriter = new DataFileWriter<EmployeeFile>(writer);

			dataFileWriter.create(employeeFile1.getSchema(), new File("src/main/resources/avro/employeeFile.avro"));
			dataFileWriter.append(employeeFile1);
			dataFileWriter.append(employeeFile2);
			dataFileWriter.append(employeeFile3);
			dataFileWriter.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
