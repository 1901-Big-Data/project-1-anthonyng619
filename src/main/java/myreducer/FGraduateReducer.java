package myreducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FGraduateReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	@Override
	public void reduce(Text value, Iterable<DoubleWritable> list, Context context) throws IOException, InterruptedException {
		double sum = 0.0;
		double stdDev = 0.0;
		double count = 0.0;
		List<DoubleWritable> cache = new ArrayList<DoubleWritable>();
		
		for(DoubleWritable val : list) {
			cache.add(val);
			sum += val.get();
			count++;
		}
		if(count == 0.0) {
			return;
		}
		double avg = sum / count;
		
		for(DoubleWritable num: cache) {
            stdDev += Math.pow(num.get() - avg, 2);
        }
		
		double stdDevResult = Math.sqrt(stdDev/count);
		if(avg - stdDevResult > 30.0) {
			return;
		}
		String retVal = new String("Average: "+avg + ", Standard Dev: " + stdDevResult);
		
		context.write(value, new Text(retVal));
	}
}
