package myreducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MEmploymentReducer extends Reducer<Text, DoubleWritable, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> list, Context context) throws IOException, InterruptedException {
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
		
		DecimalFormat df = new DecimalFormat("#0.000");
		
		String retVal = new String("Avg: " + df.format(avg) +" StdDev: " + df.format(stdDevResult));
		
		// Key = year, Value = global average change that year
		context.write(new Text(key + "   "), new Text(retVal));

	}
}
