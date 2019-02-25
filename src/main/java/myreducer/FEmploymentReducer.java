package myreducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FEmploymentReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> list, Context context) throws IOException, InterruptedException {
		boolean first = true;
		double lastNum = 0.0;
		List<Double> differences = new ArrayList<Double>();
		
		for(DoubleWritable val : list) {
			if(first) {
				first = false;
				lastNum = val.get(); 
			} else {
				double rateIncreased = val.get() - lastNum;
				differences.add(rateIncreased);
				lastNum = val.get();
			}
		}
		if(differences.isEmpty()) return;

		StringBuilder changes = new StringBuilder();
		
		for(double val : differences) {
			changes.append(val+",");
		}
		
		String retVal = new String("Average Increase: " + changes);
		
		context.write(key, new Text(retVal));

	}
}
