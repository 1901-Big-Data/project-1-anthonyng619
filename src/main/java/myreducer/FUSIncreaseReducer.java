package myreducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FUSIncreaseReducer extends Reducer<Text, DoubleWritable, Text, Text>{
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
		double sum = 0.0;
		for(double val : differences) {
			sum += val;
		}
		double avgIncrease = sum/((double)differences.size());
		
		String retVal = new String("Average Increase: " + avgIncrease);
		
		context.write(key, new Text(retVal));
		
		//context.write(key, new Text("1"));
	}
}
