import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;


public class UBERStudent20180990 {

	public static class TripVehicleCountMapper extends Mapper<Object, Text, Text, Text>
	{
    		private Text word = new Text();
    		private Text outputValue = new Text();

    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String region = value.toString().split(",")[0];
			String date = value.toString().split(",")[1];
			String data = value.toString().split(",")[2] + "," + value.toString().split(",")[3]; 
			//System.out.println(data);

			int m = Integer.parseInt(date.split("/")[0]);
			int d = Integer.parseInt(date.split("/")[1]);
			int y = Integer.parseInt(date.split("/")[2]);

			LocalDate ld = LocalDate.of(y, m, d);

			DayOfWeek dayOfWeek = ld.getDayOfWeek(); // DayOfWeek

			String day = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();
			//System.out.println(day);
			
			String str = region + "," + day;
			//System.out.println(str);
            		word.set(str);

        		outputValue.set(data);
       			context.write(word, outputValue);

    		}
	}


	public static class TripVehicleCountReducer extends Reducer<Text,Text,Text,Text>
    	{
            	private Text result = new Text();

            	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
            	{
                    	int tripSum = 0;
                    	int vehicleSum = 0;
                    	for (Text val : values)
                    	{
				String[] data = val.toString().split(",");
				tripSum += Integer.parseInt(data[0]);
				vehicleSum += Integer.parseInt(data[1]);
                    	}
			String outputValue = String.format("%d,%d", tripSum, vehicleSum); 
			result.set(outputValue);
                    	context.write(key, result);
            	}
    	}

    	public static void main(String[] args) throws Exception
    	{
            	Configuration conf = new Configuration();
            	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            	if (otherArgs.length != 2)
            	{
                    	System.err.println("Usage: tripVehiclecount <in> <out>");
                    	System.exit(2);
            	}
            	Job job = new Job(conf, "tripVehicle count");
            	job.setJarByClass(UBERStudent20180990.class);
            	job.setMapperClass(TripVehicleCountMapper.class);
            	job.setCombinerClass(TripVehicleCountReducer.class);
            	job.setReducerClass(TripVehicleCountReducer.class);
            	job.setOutputKeyClass(Text.class);
            	job.setOutputValueClass(Text.class);

            	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            	FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
            	System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
}

