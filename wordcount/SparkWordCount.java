import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

public class SparkWordCount {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    
    JavaRDD<String> input = sc.textFile("/spark/wordcount");
    JavaRDD<String> words = input.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterable<String> call(String s) {
          return Arrays.asList(s.split(" "));
        }
      }
    );

    JavaPairRDD<String, Integer> counts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }
    ).reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer x, Integer y) {
          return x + y;
        }
      } 
    );

    counts.saveAsTextFile("/spark/wordcount/output201706191010");
  }
}
