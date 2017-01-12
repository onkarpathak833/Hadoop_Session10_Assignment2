package Project1.Assignment2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Task02Reducer {

public static class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			int i = 0;
			for(IntWritable val : values) {
				
				i = i+1;
			}
			
			try {
				context.write(key, new IntWritable(i));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("Error in reduce task : "+e);
				e.printStackTrace();
			}
		}
	}
	
	
	
	
}
