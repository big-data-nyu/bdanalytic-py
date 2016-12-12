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

public class Profiling_DS_Accident_Mapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private final static String tot_rows = "tot_rows";
	
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
		case 0:
			context.write(new Text(s.substring(s.lastIndexOf("/")+1)),one);
			break;
		case 2:
			context.write(new Text(s),one);
			break;
		case 16:
			context.write(new Text("v1factor_"+s),one);
			break;
		case 17:
			context.write(new Text("v2factor_"+s),one);
			break;
		case 18:
			context.write(new Text("v1type_"+s),one);
			break;
		case 19:
			context.write(new Text("v2type_"+s),one);
			break;
		}		
	}
}