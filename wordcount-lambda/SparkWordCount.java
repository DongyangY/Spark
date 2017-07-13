import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import scala.Tuple2;

import java.util.*;

public class SparkWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> input = sc.textFile("/spark/wordcount/input");
        JavaRDD<String> words = input.flatMap((s) -> Arrays.asList(s.split(" ")));

        JavaPairRDD<String, Integer> counts = words.mapToPair((s) -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((x, y) -> x + y);

        counts.saveAsTextFile("/spark/wordcount/output20170626");
    }
}
