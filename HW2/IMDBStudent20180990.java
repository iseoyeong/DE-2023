import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20180990 {
	public static class IMDB {
      		String title;
      		double avg;

      		public IMDB(String title, double avg) {
         		this.title = title;
         		this.avg = avg;
      		}
   	}


    	public static void insertIMDB(PriorityQueue q, String title, double avg, int topK) {
        	IMDB imdb_head = (IMDB) q.peek();
        	if (q.size() < topK || imdb_head.avg < avg) {
            		IMDB imdb = new IMDB(title, avg);
            		q.add(imdb);
            		if(q.size() > topK) q.remove();
        	}
    	}

	public static class IMDBComparator implements Comparator<IMDB> {
      		public int compare(IMDB x, IMDB y) {
         		if ( x.avg > y.avg ) return 1;
         		if ( x.avg < y.avg ) return -1;
         		return 0;
      		}
   	}

	public static class IMDBMapper extends Mapper<Object, Text, Text, Text> {
  		boolean movieFile = true;
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

     			Text outputKey = new Text();
     			Text outputValue = new Text();

     			String[] data = value.toString().split("::");
     			String movieId = "";  //jointKey
     			String title = "";
     			String rating = "";
     			String genre = "";

     			if(movieFile) {
        			movieId = data[0];
        			title = data[1];
        			genre = data[2];

				if (genre.contains("Fantasy")) {
              				outputKey.set(movieId);
              				outputValue.set(title);
        				context.write( outputKey, outputValue );
           			}
        		}
     			else {
        			movieId = data[1];
        			rating = data[2];
        			outputKey.set(movieId);
        			outputValue.set(rating);
        			context.write( outputKey, outputValue );
     			}
  		}

  		protected void setup(Context context) throws IOException, InterruptedException {
     			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
     			if ( filename.indexOf( "movies.dat" ) != -1 )
        			movieFile = true;
     			else
        			movieFile = false;
  			}
  		 }

   	public static class IMDBReducer extends Reducer<Text,Text,Text,DoubleWritable> {
  		private PriorityQueue<IMDB> queue ;
  		private Comparator<IMDB> comp = new IMDBComparator();
  		private int topK;

  		Text reduce_key = new Text();
  		DoubleWritable reduce_result = new DoubleWritable();

  		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

     			int sum = 0;
     			int cnt = 0;
     			double avg = 0;
     			int flag = 0;
     			String title = "";

			for (Text val : values) {
        			try {
           			sum += Integer.parseInt(val.toString());
           			cnt++;
        			} catch (NumberFormatException e) {
           				flag = 1;
           				title = val.toString();
        			}
     			}

			if (flag == 1) {
    				avg = (double)sum / cnt;
    				insertIMDB(queue, title, avg, topK);
			}

  		}
  	
		protected void setup(Context context) throws IOException, InterruptedException {
     			Configuration conf = context.getConfiguration();
     			topK = conf.getInt("topK", -1);
     			queue = new PriorityQueue<IMDB>( topK , comp);
  		}
  	
		protected void cleanup(Context context) throws IOException, InterruptedException {
     			while( queue.size() != 0 ) {
        			IMDB imdb = (IMDB) queue.remove();
        			reduce_key.set(imdb.title);
        			reduce_result.set(imdb.avg);
        			context.write(reduce_key, reduce_result);
     			}
  		}
   	}

   	public static void main(String[] args) throws Exception {
  		Configuration conf = new Configuration();
  		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

  		if (otherArgs.length != 3) {
     			System.err.println("Usage: IMDB <in> <out> <k>");
     			System.exit(2);
  		}

  		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
  		Job job = new Job(conf, "IMDBStudent20180990");

  		job.setJarByClass(IMDBStudent20180990.class);
  		job.setMapperClass(IMDBMapper.class);
  		job.setReducerClass(IMDBReducer.class);

  		job.setOutputKeyClass(Text.class);
  		job.setOutputValueClass(Text.class);

  		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
  		System.exit(job.waitForCompletion(true) ? 0 : 1);
   	}

}
