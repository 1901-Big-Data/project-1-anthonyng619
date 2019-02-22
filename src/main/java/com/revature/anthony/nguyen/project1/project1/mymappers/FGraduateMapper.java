package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FGraduateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if(!line.contains("SE.TER.CUAT.BA.FE.ZS")) {
			return;
		}
		String[] columns = line.split(",");
		for(int i = 4; i < columns.length; i++) {
			try {
				String string = columns[i].substring(1, columns[i].length()-1);
				DoubleWritable val = new DoubleWritable(Double.parseDouble(string));
				context.write(new Text(columns[0].substring(1, columns[0].length()-1)), val);
			} catch(NumberFormatException e) {
				
			}
		}
		
	}
}
