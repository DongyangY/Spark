import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;

public class SparkSQLJSON {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("SparkSQLJSON");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlc = new SQLContext(sc);

    String path = "/spark/sparksql";
    DataFrame df = sqlc.read().json(path);

    df.printSchema();
  }
}
