package com.revature.anthony.nguyen.project1.project1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.anthony.nguyen.project1.project1.mymappers.FGraduateMapper;
import com.revature.anthony.nguyen.project1.project1.mymappers.FUSIncreaseMapper;
import com.revature.anthony.nguyen.project1.project1.mymappers.MEmploymentMapper;

import myreducer.FGraduateReducer;
import myreducer.FUSIncreaseReducer;
import myreducer.MEmploymentReducer;

public class Driver {
	public static void main(String[] args) throws Exception {
		
		/*
		 * Need to be at least length two for input and output data file paths.
		 */
		if(args.length != 2) {
			System.out.printf("Usage: WordCount <input_dir> <output_dir>\n");
			System.exit(-1);
		}
		
		/*
		Job job_q1 = new Job();
		
		job_q1.setJarByClass(Driver.class);
		
		job_q1.setJobName("Female Graduates");
		
		FileInputFormat.setInputPaths(job_q1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_q1, new Path(args[1]));
		
		job_q1.setMapperClass(FGraduateMapper.class);
		job_q1.setReducerClass(FGraduateReducer.class);
		
		job_q1.setMapOutputKeyClass(Text.class);
		job_q1.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job_q1.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		*/
		
		/*
		Job job_q2 = new Job();
		
		job_q2.setJarByClass(Driver.class);
		
		job_q2.setJobName("Female Education Increasing Rate Since 2000");
		
		FileInputFormat.setInputPaths(job_q2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_q2, new Path(args[1]));
		
		job_q2.setMapperClass(FUSIncreaseMapper.class);
		job_q2.setReducerClass(FUSIncreaseReducer.class);
		
		job_q2.setMapOutputKeyClass(Text.class);
		job_q2.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job_q2.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		*/
		
		// Question 3
		
		Job job_q3 = new Job();
		
		job_q3.setJarByClass(Driver.class);
		
		job_q3.setJobName("Male Employment Changes since 2000");
		
		FileInputFormat.setInputPaths(job_q3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_q3, new Path(args[1]));
		
		job_q3.setMapperClass(MEmploymentMapper.class);
		job_q3.setReducerClass(MEmploymentReducer.class);
		
		job_q3.setMapOutputKeyClass(Text.class);
		job_q3.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job_q3.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
		
		/*
		Job job_q4 = new Job();
		
		job_q4.setJarByClass(Driver.class);
		
		job_q4.setJobName("Male Employment Changes since 2000");
		
		FileInputFormat.setInputPaths(job_q4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_q4, new Path(args[1]));
		
		job_q4.setMapperClass(MEmploymentMapper.class);
		job_q4.setReducerClass(MEmploymentReducer.class);
		
		job_q4.setMapOutputKeyClass(Text.class);
		job_q4.setMapOutputValueClass(DoubleWritable.class);
		
		boolean success = job_q4.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		*/
		
	}
}
