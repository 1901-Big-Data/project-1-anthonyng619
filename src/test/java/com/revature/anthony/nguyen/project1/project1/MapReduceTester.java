package com.revature.anthony.nguyen.project1.project1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.anthony.nguyen.project1.project1.mymappers.FGraduateMapper;

import myreducer.FGraduateReducer;

public class MapReduceTester {
	/*
	 * Declare harnesses that let you test a mapper, a reducer, and
	 * a mapper and a reducer working together.
	 */
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		FGraduateMapper mapper = new FGraduateMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		FGraduateReducer reducer = new FGraduateReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Text>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		mapDriver.withInput(new LongWritable(1), new Text("\"Sao Tome and Principe\",\"STP\",\"Educational attainment at least Bachelor's or equivalent population 25+ female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"2.17914\",,"));

			System.out.println("\"Sao Tome and Principe\",\"STP\",\"Educational attainment at least Bachelor's or equivalent population 25+ female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"2.17914\",,");
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		mapDriver.withOutput(new Text("Sao Tome and Principe"), new DoubleWritable(2.17914));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(3.00));
		values.add(new DoubleWritable(4.00));
		
		reduceDriver.withInput(new Text("Sao Tome and Principe"), values);
		
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		reduceDriver.withOutput(new Text("Sao Tome and Principe"), new Text("Average: 3.5, Standard Dev: 0.0"));

		/*
		 * Run the test.
		 */
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Sao Tome and Principe\",\"STP\",\"Educational attainment at least Bachelor's or equivalent population 25+ female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"3.0\",\"5.0\",\"4.0\",\"2.5\""));

		/*
		 * The expected output (from the reducer) is "cat 2", "dog 1". 
		 */
		mapReduceDriver.addOutput(new Text("Sao Tome and Principe"), new Text("Average: 4.0, Standard Dev: 0.0"));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
}
