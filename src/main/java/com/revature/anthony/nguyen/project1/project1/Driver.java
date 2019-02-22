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

import myreducer.FGraduateReducer;

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
		 * Instantiate job object
		 */
		Job job_q1 = new Job();
		
		job_q1.setJarByClass(FGraduateMapper.class);
		
		job_q1.setJobName("Female Graduates");
		
		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job_q1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_q1, new Path(args[1]));
		
		/*
		 * Specify the mapper and reducer classes.
		 */
		job_q1.setMapperClass(FGraduateMapper.class);
		job_q1.setReducerClass(FGraduateReducer.class);
		
		/*
		 * Specify the job's output key and value classes.
		 */
		job_q1.setOutputKeyClass(Text.class);
		job_q1.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job_q1.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
