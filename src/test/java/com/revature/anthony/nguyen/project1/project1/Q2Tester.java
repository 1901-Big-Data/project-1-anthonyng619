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
import com.revature.anthony.nguyen.project1.project1.mymappers.FUSIncreaseMapper;

import myreducer.FGraduateReducer;
import myreducer.FUSIncreaseReducer;

public class Q2Tester {
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
		FUSIncreaseMapper mapper = new FUSIncreaseMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		FUSIncreaseReducer reducer = new FUSIncreaseReducer();
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
		testString[2] = "\"Educational, attainment female\"";
		testString[3] = "\"SE.TER.CUAT.MS.FE.ZS\"";
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
		mapDriver.withOutput(new Text("Educational, attainment female"), new DoubleWritable(20.00));
		mapDriver.withOutput(new Text("Educational, attainment female"), new DoubleWritable(22.00));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {

		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(26.00));
		values.add(new DoubleWritable(26.00));
		
		reduceDriver.withInput(new Text("Educational attainment female"), values);
		
		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		reduceDriver.withOutput(new Text("Educational attainment female"), new Text("Average Increase: 0.000"));

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
		testString[2] = "\"Educational attainment female\"";
		testString[3] = "\"SE.TER.CUAT.MS.FE.ZS\"";
		testString[44] = "\"20.00\"";
		testString[45] = "\"22.00\"";
		
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
		mapReduceDriver.addOutput(new Text("Educational attainment female"), new Text("Average Increase: 2.000"));

		/*
		 * Run the test.
		 */
		mapReduceDriver.runTest();
	}
	
}
