package com.qi.secondSort;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.qi.bean.WordWritable;
import com.qi.topN.TopN;
import com.qi.topN.TopN.MyMApper;
import com.qi.topN.TopN.MyReducer;

public class SecondSortMR {

	public static class SecondSortMapper extends Mapper<LongWritable, Text, WordWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WordWritable, Text>.Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\\s+");
			WordWritable wd = new WordWritable();
			wd.setWord(line[0]);
			wd.setNum(Integer.parseInt(line[1].split("---")[1]));
			context.write(wd, new Text(line[1].split("---")[0]));
		}

	}

	public static class SecondSortReducer extends Reducer<WordWritable, Text, Text, Text> {

		ArrayList<String> list = new ArrayList<>();

		@Override
		protected void reduce(WordWritable key, Iterable<Text> values,
				Reducer<WordWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			for (Text text : values) {
				String word = key.getWord();
				int num = key.getNum();
				String fileName=text.toString();
				list.add(fileName + "--->" + num);
				if (list.size() == 4) {
					list.remove(3);
				}
			}
			StringBuilder sb=new StringBuilder();
			for (String value : list) {
				
				sb.append(value);
				sb.append("||");
			}
			context.write(new Text(key.getWord()+"\t"), new Text(sb.toString().substring(0, sb.length()-2)));
			list.clear();

		}

	}

	public static class SecondSort extends WritableComparator {

		public SecondSort() {
			super(WordWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			WordWritable awd = (WordWritable) a;
			WordWritable bwd = (WordWritable) b;

			int result = awd.getWord().compareTo(bwd.getWord());
			if (result != 0) {
				return result;
			} else {
				return -awd.getNum().compareTo(bwd.getNum());
			}
		}

	}

	public static class SecondGroup extends WritableComparator {

		public SecondGroup() {
			super(WordWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			WordWritable awd = (WordWritable) a;
			WordWritable bwd = (WordWritable) b;

			return awd.getWord().compareTo(bwd.getWord());

		}

	}

	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "SecondSortMR");
			job.setJarByClass(SecondSortMR.class);
			
			job.setMapperClass(SecondSortMapper.class);
			job.setReducerClass(SecondSortReducer.class);

			job.setMapOutputKeyClass(WordWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setSortComparatorClass(SecondSort.class);
			job.setGroupingComparatorClass(SecondGroup.class);
			
			Path inputPath = new Path("/secondSordInput/word_file_count");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/secondSordOutput");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);
			
			boolean flag = job.waitForCompletion(true);

			System.exit(flag ? 0 : 1);
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
	

}
