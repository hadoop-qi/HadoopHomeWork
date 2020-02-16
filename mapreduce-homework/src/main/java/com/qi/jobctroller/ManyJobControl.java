package com.qi.jobctroller;

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

import com.qi.bean.CountWritable;
import com.qi.mapreduce.CombineFile;
import com.qi.mapreduce.CombineFile.CombineFileMapper;
import com.qi.mapreduce.CombineFile.CombineFileReducer;
import com.qi.mapreduce.WordCound;
import com.qi.mapreduce.WordCound.WordCoundMapper;
import com.qi.mapreduce.WordCound.WordCoundReducer;

public class ManyJobControl {

	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job1 = Job.getInstance(conf, "WordCount");
			job1.setJarByClass(WordCound.class);
			
			job1.setMapperClass(WordCoundMapper .class);
			job1.setReducerClass(WordCoundReducer.class);
			// reduce 数量为 0，就会把 map 的输出给放到 hdfs 上
			job1.setNumReduceTasks(1);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(CountWritable.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job1, new Path("/input/apr License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/Changelog.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/CyrusSASL License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/OpenSSL License.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/Subversion license.txt"));
			FileInputFormat.addInputPath(job1, new Path("/input/TortoiseSVN License.txt"));
			
			Path outputDir = new Path("/WordCound-output");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job1, outputDir);
			
			
		
			Job job2 = Job.getInstance(conf, "WordCount");
			job2.setJarByClass(CombineFile.class);
			
			job2.setMapperClass(CombineFileMapper.class);
			job2.setReducerClass(CombineFileReducer.class);
			// reduce 数量为 0，就会把 map 的输出给放到 hdfs 上
			job2.setNumReduceTasks(1);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			
			FileInputFormat.addInputPath(job2, new Path("/WordCound-output/part-r-00000"));
			Path outputath = new Path("/CombineFile-output");
			FileSystem.get(conf).delete(outputath, true);
			FileOutputFormat.setOutputPath(job2, outputath);
			
			
			
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
