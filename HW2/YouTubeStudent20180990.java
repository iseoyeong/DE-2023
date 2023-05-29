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

public class YouTubeStudent20180990 {

   public static class Youtube {
      	String category;
      	double avg;
      	
	public Youtube(String category, double avg) {
        	this.category = category;
         	this.avg = avg;
      	}
   }

   public static void insertYoutube(PriorityQueue q, String category, double avg, int topK) {
   	Youtube youtube_head = (Youtube) q.peek();

      	if (q.size() < topK || youtube_head.avg < avg ) {
	      Youtube y = new Youtube(category, avg);
	      q.add(y);
	      if( q.size() > topK ) q.remove();
      	}
   }

   public static class YoutubeComparator implements Comparator<Youtube> {
      public int compare(Youtube x, Youtube y) {
         if ( x.avg > y.avg ) return 1;
         if ( x.avg < y.avg ) return -1;
         return 0;
      }
   }
   
   public static class YoutubeMapper extends Mapper<Object, Text, Text, Text> {
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         Text outputKey = new Text();
         Text outputValue = new Text();
         
	 String[] data = value.toString().split("\\|");
	 String category = data[3];
	 System.out.println(category);
	 String avg = data[6];
	 System.out.println(avg);

         outputKey.set(category);
         outputValue.set(avg);
         context.write(outputKey, outputValue);
      }   
   }
   
   public static class YoutubeReducer extends Reducer<Text,Text,Text,DoubleWritable>
   {
      private PriorityQueue<Youtube> queue ;
      private Comparator<Youtube> comp = new YoutubeComparator();
      private int topK;
      
      Text reduce_key = new Text();
      DoubleWritable reduce_result = new DoubleWritable();
      
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         double sum = 0;
         int cnt = 0;
         double avg = 0;
         String category = key.toString();

         for (Text val : values) {
            sum += Double.parseDouble(val.toString());
            cnt++;
         } 
         avg = sum / cnt;
         insertYoutube(queue, category, avg, topK);
         
      }

      protected void setup(Context context) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         topK = conf.getInt("topK", -1);
         queue = new PriorityQueue<Youtube>(topK , comp);
      }

      protected void cleanup(Context context) throws IOException, InterruptedException {
         while( queue.size() != 0 ) {
            Youtube y = (Youtube) queue.remove();
            reduce_key.set(y.category);
            reduce_result.set(y.avg);
            context.write(reduce_key, reduce_result);
         }
      }
   }

   public static void main(String[] args) throws Exception
   {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      
      if (otherArgs.length != 3)
      {
         System.err.println("Usage: Youtube <in> <out> <k>");
         System.exit(2);
      }
      
      conf.setInt("topK", Integer.parseInt(otherArgs[2]));
      Job job = new Job(conf, "Youtube");
   
      job.setJarByClass(YouTubeStudent20180990.class);
      job.setMapperClass(YoutubeMapper.class);
      job.setReducerClass(YoutubeReducer.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
   
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
