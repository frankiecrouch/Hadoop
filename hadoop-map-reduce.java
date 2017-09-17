import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Date;
import java.util.Scanner;
import java.util.Calendar;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GPScount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text GPS_key = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	//first check that the line is complete/contains data we what to read 
      /*** count should be equal to 6 ***/
    	int count_commas = StringUtils.countMatches(value.toString(), ",");
    	
    	if(count_commas == 6){
    		//use scanner function to read along the line with a comma delimiter
    		Scanner line = new Scanner (value.toString());
    		//set the delimiter
    		line.useDelimiter(",");  		
    		
    		//set the centre point of the circle and radius
    		double centre_x = 39.9765778;
    		double centre_y = 116.3360511;
    		double r = 0.0045; //assume one degree is equivalent to 111km => 500m = 0.0045 degrees
    		
        //initiate variables
    		double latitude, longitude;
    		latitude = longitude = 0;
    		String date_string, time;
    		date_string = time = null;
    		
        //iterate through the line and set the latitude, longitude, date and time (e.g. skip some of the data)
    		while (line.hasNext()){
    			latitude = Double.parseDouble(line.next()); 
    			longitude = Double.parseDouble(line.next());
      		line.next(); line.next(); line.next(); // skip some of the fields
      		date_string = line.next(); //get the date
      		time = line.next(); // get the time
    		}//end of while
        line.close();//close the scanner
	      		
	      		//check if coordinates are in the circle and if they are => MAP
	      		if ((longitude - centre_y)*(longitude - centre_y) + 
	      				(latitude - centre_x)*(latitude - centre_x) < r*r){
	      			
          			//get the file path of the line being read, to get the employee number
          			Path filePath = ((FileSplit)context.getInputSplit()).getPath();
            		//use scanner to read through the file path with / as the delimiter
            		Scanner employee = new Scanner (filePath.toString());
            		employee.useDelimiter("/");
            		
            		String employee_number = null;
            		while(employee.hasNext()){
            			employee.next();employee.next();employee.next();//skip part of the filepath
            			employee.next();employee.next();employee.next();
            			employee_number = employee.next(); //employee number is the 7th elements
            			employee.next();employee.next();	
            		}
	        		   employee.close();//close the scanner
	      			
      	        		//get the day of the week from the date
      	        		String day_key = null;
      	        		try{
      	        			SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");
      	        			Date date = format.parse(date_string);
      	        			Calendar cal = GregorianCalendar.getInstance();
      		        		cal.setTime(date);
      		        		int date_int = cal.get(Calendar.DAY_OF_WEEK);
      		        		day_key = Integer.toString(date_int); // this is the day of the week to add to the key
      	        		}
      	        		catch (java.text.ParseException e){
      	        			e.printStackTrace();
      	        		}
	        		
	        		//create the key to be mapped e.g. 6 digits, the first = the employee number, 
              //next two = the hour and last digit = the day of the week
              String hour_key = time.substring(0,2);     		
	        		String line_key = employee_number + hour_key + day_key;

              //data = the key, this is mapped to 1
	      			GPS_key.set(line_key);
	    		    context.write(GPS_key, one); 
	    		    
	      		}//end of if(in the circle) statement    	    		
	
    	}//end of first if(6 commas in a line) statement
    	
   }//end of map method
    
}//end of TokenizerMapper


  //reduce method - this is unchanged from the WordCount code
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  //main method
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    //Delete the output file if it exists
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(new Path(args[0]))){
    	fs.delete(new Path(args[0]), true);
    }
    
    //set up the job
    Job job = Job.getInstance(conf, "gps count");
    job.setJarByClass(GPScount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //input files => set file path for all 182 employees
    for (int i = 0; i < 181; i++){    	
    	String file_number = String.format("%03d", i);      	
    	FileInputFormat.addInputPaths(job, "/home/ubuntu/workspace/GPScount/Data/"+ file_number + "/Trajectory");
    }
    
    //create output file
    FileOutputFormat.setOutputPath(job, new Path(args[0])); 
    
    System.exit(job.waitForCompletion(true)?0:1);

  }//end of main

}//end of GPScount