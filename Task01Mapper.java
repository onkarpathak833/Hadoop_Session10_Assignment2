package Project1.Assignment2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task01Mapper {

	
	public static class TaskMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text line, Context context) {
			int i =1;
			String value = line.toString();
			if(value!=null && value.length()>0) {
				String []allValues = value.split(",");
				if(allValues.length > 11) {
					
					String fbiCode = allValues[14];
					
					try {
						context.write(new Text(fbiCode), new IntWritable(1));
					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println(" *** Error in Map Function."+e);
						e.printStackTrace();
					}
				}
				
				
				
			}
			
			
		}
		
		
	}
}
