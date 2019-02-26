package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.revature.anthony.nguyen.project1.project1.DataHeader;

public class FEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		if(!(columns[DataHeader.getIndex("Indicator Code")].contains("SL.EMP.TOTL.SP.MA.ZS"))) {
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
				context.write(new Text(columns[DataHeader.getIndex("Country Code")]), new DoubleWritable(difference));
			} catch (NumberFormatException e) {
				
			}
		}
	}
}
