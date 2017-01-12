package Project1.Assignment2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task04Mapper {

public static class Task4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@SuppressWarnings("deprecation")
		public void map(LongWritable key, Text line, Context context) {
			int i =1;
			String value = line.toString();
			if(value!=null && value.length()>0) {
				String []allValues = value.split(",");
				if(allValues.length > 9) {
					
					String isArrest = allValues[8];
					isArrest = isArrest.trim();
					String dateString = allValues[2];
					dateString = dateString.trim();
					
					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
					
					Date crimeDate = null;
					try {
						crimeDate = sdf.parse(dateString);
					} catch (ParseException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					System.out.println("** Date is "+dateString);
					
					System.out.println("** Date Object is "+crimeDate+" and arrest is "+isArrest);
					
					
					try {
						if("TRUE".equalsIgnoreCase(isArrest)) {
							System.out.println("** Arrest is true.");
							if(isDateBetween(crimeDate)) {
								context.write(new Text("ARREST"), new IntWritable(1));
							}
							
						}
						
						
					
					} catch (IOException | InterruptedException e) {
						// TODO Auto-generated catch block
						System.out.println(" *** Error in Map Function."+e);
						e.printStackTrace();
					} catch (ParseException e) {
						System.out.println("Exception while parsing input date "+e);
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				
				
			}
			
			
		}
		
		@SuppressWarnings("deprecation")
		public boolean isDateBetween(Date date) throws ParseException {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
			Date startDate = sdf.parse("10/01/2014 12:00:00 AM");
			
			Date endDate = sdf.parse("10/31/2015 11:59:59 PM");
			
			System.out.println("Custom Dates : "+startDate+" and "+endDate);
			
			if(date.after(startDate) && date.before(endDate)) {
				System.out.println("** Valid Date :"+date);
				return true;
			}
			return false;
		}

}
	
	
}
