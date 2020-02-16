package com.qi.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordWritable implements WritableComparable<WordWritable>{

	private String word;
	
	private Integer num;
	
	
	public WordWritable() {
		super();
	}

	public WordWritable(String word, Integer num) {
		super();
		this.word = word;
		this.num = num;
	}


	@Override
	public String toString() {
		return "WordWritable [word=" + word + ", num=" + num + "]";
	}

	

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getNum() {
		return num;
	}

	public void setNum(Integer num) {
		this.num = num;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(num);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		word=in.readUTF();
		num=in.readInt();
	}

	@Override
	public int compareTo(WordWritable o) {
		
		return word.compareTo(o.getWord());
	}

}
