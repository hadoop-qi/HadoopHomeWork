package com.qi.totalsort;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MakeNumber {
	
	public static void main(String[] args) {
		try(FileWriter fileWriter=new FileWriter("e:/number.log");
				BufferedWriter bufferedWriter=new BufferedWriter(fileWriter)) {
			
			int size=10000;
			for (int i = 0; i < size; i++) {
				int number=(int)(Math.random()*size);
				bufferedWriter.write(String.valueOf(number));
				bufferedWriter.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
