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

import com.revature.anthony.nguyen.project1.project1.mymappers.MEmploymentMapper;

import myreducer.MEmploymentReducer;

public class Q3Tester {
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
		MEmploymentMapper mapper = new MEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		MEmploymentReducer reducer = new MEmploymentReducer();
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

		String[] testString = new String[50];
		testString[0] = "\"United States of America\"";
		testString[1] = "\"USA\"";
		testString[2] = "\"Employment male\"";
		testString[3] = "\"EMP.TER.CUAT.MS.MA.ZS\"";
		testString[46] = "\"20.00\"";
		testString[47] = "\"22.00\"";
		
		String gogo = "";
		for(int i = 0; i < testString.length; i++) {
			if(testString[i] == null) {
				if(i == 0) {
					gogo += "\"\"";
				} else {
					gogo += "\"\",";
				}
			} else {
				gogo += testString[i] +",";
			}
		}
		
		System.out.println(gogo);
		
		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		mapDriver.withInput(new LongWritable(1), new Text(gogo));

		
		
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		mapDriver.withOutput(new Text("2003"), new DoubleWritable(2.0));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(2.500));
		values.add(new DoubleWritable(3.500));
		
		reduceDriver.withInput(new Text("2003"), values);
		
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		reduceDriver.withOutput(new Text("2003"), new Text("Average Increase: 3.000 Standard Deviation: 0.500"));

		/*
		 * Run the test.
		 */
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {

		String[] testString = new String[48];
		testString[0] = "\"United States of America\"";
		testString[1] = "\"USA\"";
		testString[2] = "\"Employment male\"";
		testString[3] = "\"EMP.TER.CUAT.MS.MA.ZS\"";
		testString[44] = "\"20.00\"";
		testString[45] = "\"22.00\"";
		testString[46] = "\"24.00\"";
		
		String gogo = "";
		for(int i = 0; i < testString.length; i++) {
			if(testString[i] == null) {
				if(i == 0) {
					gogo += "\"\"";
				} else {
					gogo += "\"\",";
				}
			} else {
				gogo += testString[i] +",";
			}
		}
		
		System.out.println(gogo);
		mapReduceDriver.withInput(new LongWritable(1), new Text(gogo));

		/*
		 * The expected output (from the reducer) is "cat 2", "dog 1". 
		 */
		mapReduceDriver.addOutput(new Text("2001"), new Text("Average Increase: 2.000 Standard Deviation: 0.000"));
		mapReduceDriver.addOutput(new Text("2002"), new Text("Average Increase: 4.000 Standard Deviation: 0.000"));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
	
}
