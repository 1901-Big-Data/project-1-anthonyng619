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
				columns[DataHeader.getIndex("Indicator Code")].toLowerCase().contains("se") && columns[DataHeader.getIndex("Indicator Code")].toLowerCase().contains("fe"))) {
			return;
		}
		
		double lastNum = 0.0;
		for(int i =DataHeader.getIndex("1999"); i < columns.length; i++) {
			try {
				if(lastNum == 0.0) {
					lastNum = Double.parseDouble(columns[i].substring(1, columns[i].length()-1));
					continue;
				}
				
				String string = columns[i].substring(1, columns[i].length()-1);
				double val = Double.parseDouble(string);
				
				double difference = val-lastNum;
				
				String year = DataHeader.getLabel(i);
				
				// Key = country, Value = percentage
				context.write(new Text(columns[DataHeader.getIndex("Indicator Name")]), new DoubleWritable(difference));
			} catch (NumberFormatException e) {
				
			}
		}
	}
}
