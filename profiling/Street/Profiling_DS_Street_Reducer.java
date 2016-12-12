import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Profiling_DS_Street_Reducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	private final static String tot_rows = "tot_rows";
	private final static String length = "length";
	private final static String width = "width";
	private final static String min_length = "min_length";
	private final static String max_length = "max_length";
	private final static String min_width = "min_width";
	private final static String max_width = "max_width";
	private final static String min_rating = "min_rating";
	private final static String max_rating = "max_rating";
	private final static String primary_usage = "primary_usage";
	private final static String secondary_usage = "secondary_usage";
	private final static String local_usage = "local_usage";
	private final static String unknown_usage = "unknown_usage";
	private final static String fair_rating = "fair_rating";
	private final static String good_rating = "good_rating";
	private final static String poor_rating = "poor_rating";

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		if(!key.toString().equals(length)&&!key.toString().equals(width)){
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}else{
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			for (IntWritable val : values) {
				if(val.get()<min)
					min = val.get();
				else if(val.get()>max)
					max = val.get();
			}
			context.write(new Text(key.toString()+"_max"),new IntWritable(max));
			context.write(new Text(key.toString()+"_min"),new IntWritable(min));
		}
	
		
	}
}