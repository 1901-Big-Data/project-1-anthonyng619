package com.revature.anthony.nguyen.project1.project1;
import java.util.ArrayList;

public class DataHeader {
	public volatile static ArrayList<String> header = new ArrayList<String>();
	static {
		String headerStr = "Country Name,Country Code,Indicator Name,Indicator Code,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,";
		for(String s : headerStr.split(",")) {
			header.add(s);
		}
	}
	
	public static int getIndex(String string) {
		return header.indexOf(string);
	}
	
	public static String getLabel(int index) {
		return header.get(index);
	}

}
