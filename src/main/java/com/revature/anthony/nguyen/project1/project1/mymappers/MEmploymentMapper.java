package com.revature.anthony.nguyen.project1.project1.mymappers;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.anthony.nguyen.project1.project1.DataHeader;

public class MEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	public volatile static ArrayList<String> header = new ArrayList<String>();
	{
		String headerStr = "Country Name,Country Code,Indicator Name,Indicator Code,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,";
		for(String s : headerStr.split(",")) {
			header.add(s);
		}
	}
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
