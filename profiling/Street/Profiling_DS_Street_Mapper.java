import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Profiling_DS_Street_Mapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable zero = new IntWritable(0);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private final static String tot_rows = "tot_rows";
	private final static String length = "length";
	private final static String width = "width";
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int index = 0;
		StringTokenizer itr = new StringTokenizer(value.toString(), ",");
		while (itr.hasMoreTokens()) {
			String currenttoken = itr.nextToken();
			try {
				helper(index,currenttoken,context);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
			}
			index++;
		}
		context.write(new Text(tot_rows), one);
	}
	
	private static void helper(int index, String s, Context context) throws NumberFormatException, IOException, InterruptedException {
		switch(index){
		case 4: 
			context.write(new Text(length),new IntWritable(Integer.parseInt(s)));
			break;
		case 6:
			context.write(new Text(width),new IntWritable(Integer.parseInt(s)));
			break;
		case 7:
			context.write(new Text(s),one);
			break;
		case 9:
			context.write(new Text(s),one);
			break;
		}		
	}
}