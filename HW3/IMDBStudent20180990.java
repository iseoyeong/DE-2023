import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.*;
import java.util.*;

public final class ModifiedIMDBStudent20180990 {

	public static void main(String[] args) throws Exception {

    	if (args.length < 2) {
        	System.err.println("Usage: ModifiedIMDBStudent20180990 <in-file> <out-file>");
        	System.exit(1);
    	}

    	SparkSession spark = SparkSession
        	.builder()
        	.appName("ModifiedIMDBStudent20180990")
        	.getOrCreate();

    	JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    	FlatMapFunction<String, String> flatMapFunc = new FlatMapFunction<String, String>() {
         	public Iterator<String> call(String s) {
                    	ArrayList<String> result = new ArrayList<String>();
                    	String [] split = s.split("::");
                    	StringTokenizer tokenizer = new StringTokenizer(split[2], "|");

                    	while (tokenizer.hasMoreTokens()) {
                            	String genre = tokenizer.nextToken();
                            	result.add(genre);
                    	}
                	return result.iterator();
            	}
    	};
   	 
    	JavaRDD<String> words = lines.flatMap(flatMapFunc);
    	PairFunction<String, String, Integer> pairFunc = new PairFunction<String, String, Integer>() {
        	public Tuple2<String, Integer> call(String s) {
                	return new Tuple2(s, 1);
        	}
    	};

    	JavaPairRDD<String, Integer> ones = words.mapToPair(pairFunc);

    	Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
        	public Integer call(Integer x, Integer y) {
                	return x + y;
        	}
    	};

    	JavaPairRDD<String, Integer> counts = ones.reduceByKey(reduceFunc);

    	JavaRDD<String> resultRdd = counts.map(x -> x._1 + " " + x._2);

    	resultRdd.saveAsTextFile(args[1]);
    	spark.stop();
    	}
}

