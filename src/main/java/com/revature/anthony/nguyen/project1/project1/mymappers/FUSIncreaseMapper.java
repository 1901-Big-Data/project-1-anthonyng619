package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.anthony.nguyen.project1.project1.DataHeader;

public class FUSIncreaseMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	/**
	 * Extract all female education in the US since the year 2000 and passes each type of indicator name
	 * as a key
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		if(!(columns[DataHeader.getIndex("Country Code")].toLowerCase().contains("usa") &&
				columns[DataHeader.getIndex("Indicator Code")].toLowerCase().contains("se") && columns[DataHeader.getIndex("Inidicator Code")].toLowerCase().contains("fe"))) {
			return;
		}
		for(int i = DataHeader.getIndex("2000"); i < columns.length; i++) {
			try {
				String string = columns[i].substring(1, columns[i].length()-1);
				DoubleWritable val = new DoubleWritable(Double.parseDouble(string));
				context.write(new Text(columns[DataHeader.getIndex("Indicator Name")].substring(1, columns[DataHeader.getIndex("Indicator Name")].length()-1)), val);
			} catch (NumberFormatException e) {
				//System.out.println("Nope");
			}
		}
		/*if(columns[1].toLowerCase().contains("usa")) {
			context.write(new Text(columns[1]), new DoubleWritable(1.0));
		}*/
	}
}
