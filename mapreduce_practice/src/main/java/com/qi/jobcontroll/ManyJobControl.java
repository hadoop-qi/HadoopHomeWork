package com.qi.jobcontroll;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.qi.combine.CombinFile;
import com.qi.combine.CombinFile.CombinFileMapper;
import com.qi.combine.CombinFile.CombinFileReducer;
import com.qi.invertedindex.InvertedIndex;
import com.qi.invertedindex.InvertedIndex.InvertedIndexMapper;
import com.qi.invertedindex.InvertedIndex.InvertedIndexReducer;

public class ManyJobControl {

	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job1 = Job.getInstance(conf, "CombinFile");
			job1.setJarByClass(CombinFile.class);
			
			job1.setMapperClass(CombinFileMapper.class);
			job1.setReducerClass(CombinFileReducer.class);
			// reduce 数量为 0，就会把 map 的输出给放到 hdfs 上
		
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job1, new Path("/input/apr License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/Changelog.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/CyrusSASL License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/OpenSSL License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/Subversion license.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/TortoiseSVN License.txt"));
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			Path outputDir = new Path("/combine-output");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job1, outputDir);
			
			
			
		
			Job job2=Job.getInstance(conf, "InvertedIndex");
			
			job2.setJarByClass(InvertedIndex.class);
			
			job2.setMapperClass(InvertedIndexMapper.class);
			job2.setReducerClass(InvertedIndexReducer.class);
		
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			
			Path inputPath=new Path("/combine-output");
			FileInputFormat.addInputPath(job2, inputPath);
			
			Path outPutPath=new Path("/invertindex-output");
			FileSystem.get(conf).delete(outPutPath,true);
			FileOutputFormat.setOutputPath(job2, outPutPath);
			
			
			
			
			ControlledJob controlledJob1=new ControlledJob(conf);
			controlledJob1.setJob(job1);
			ControlledJob controlledJob2=new ControlledJob(conf);
			controlledJob2.setJob(job2);
			
			controlledJob2.addDependingJob(controlledJob1);
			
			JobControl jobControl=new JobControl("combine inverted");
			
			jobControl.addJob(controlledJob1);
			jobControl.addJob(controlledJob2);
			
			Thread jobControlThread=new Thread(jobControl);
			jobControlThread.start();
			
			while (true) {
				if (jobControl.allFinished()) {
					System.out.println("任务全部完成了");
					jobControl.stop();
					break;
				}
				if (jobControl.getFailedJobList().size()>0) {
					System.out.println("有任务失败了");
					jobControl.stop();
					break;
				}
				Thread.sleep(1000);
			}

		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
}
