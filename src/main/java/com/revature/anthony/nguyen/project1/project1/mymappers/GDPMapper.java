package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.anthony.nguyen.project1.project1.DataHeader;

public class GDPMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	/**
	 * Extract all GDP growth rate since the year 2000 with the country code as a key
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		if(!(columns[DataHeader.getIndex("Indicator Code")].contains("NY.GDP.MKTP.KD.ZG"))) {
			return;
		}
		for(int i = DataHeader.getIndex("2000"); i < columns.length; i++) {
			try {
				String string = columns[i].substring(1, columns[i].length()-1);
				DoubleWritable val = new DoubleWritable(Double.parseDouble(string));
				context.write(new Text(columns[DataHeader.getIndex("Country Code")].substring(1, columns[DataHeader.getIndex("Country Code")].length()-1)), val);
			} catch (NumberFormatException e) {
				//System.out.println("Nope");
			}
		}
	}
}
