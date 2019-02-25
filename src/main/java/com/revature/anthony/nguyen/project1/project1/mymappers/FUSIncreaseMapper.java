package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FUSIncreaseMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		if(!(columns[1].toLowerCase().contains("usa") &&
				columns[3].toLowerCase().contains("se") && columns[3].toLowerCase().contains("fe"))) {
			return;
		}
		for(int i = 5; i < columns.length; i++) {
			try {
				String string = columns[i].substring(1, columns[i].length()-1);
				DoubleWritable val = new DoubleWritable(Double.parseDouble(string));
				context.write(new Text(columns[2].substring(1, columns[2].length()-1)), val);
			} catch (NumberFormatException e) {
				//System.out.println("Nope");
			}
		}
		/*if(columns[1].toLowerCase().contains("usa")) {
			context.write(new Text(columns[1]), new DoubleWritable(1.0));
		}*/
	}
}
