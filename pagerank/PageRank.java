import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.random.*;
import scala.Tuple2;

import java.util.*;

public class Random {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("pagerank");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    if (args.length < 2) {
      System.out.println("e.g.: spark-submit --master yarn --class PageRank PageRank.jar [num_nodes] [num_iters]");
      System.exit(1);
    }

    final int num = Integer.parseInt(args[0]);
    final int iter = Integer.parseInt(args[1]);

    if (num <= 0 || iter <= 0) {
      System.out.println("invalid args");
      System.exit(1);
    }

    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      list.add(i);
    }

    JavaRDD<Integer> rdd = sc.parallelize(list);

    JavaPairRDD<Integer, List<Integer>> graph = rdd.mapToPair(
      new PairFunction<Integer, Integer, List<Integer>>() {
        @Override
        public Tuple2<Integer, List<Integer>> call(Integer key) {
          int n = (int)(Math.random() * num) + 1;
          List<Integer> list = new ArrayList<>();
          Set<Integer> set = new HashSet<>();
          for (int i = 0; i < n; i++) {
            int rand = (int)(Math.random() * num);
            if (rand == key.intValue()) {
              i--;
              continue;
            }
            set.add(rand);
          }

          for (Integer i : set) {
            list.add(i);
          }

          return new Tuple2<Integer, List<Integer>>(key, list);
        }
      }
    );

    graph.cache();

    System.out.println("----------graph----------");
    printMap(graph.collectAsMap());

    JavaPairRDD<Integer, Double> rank = rdd.mapToPair(new PairFunction<Integer, Integer, Double>() {
        @Override
        public Tuple2<Integer, Double> call(Integer key) {
          return new Tuple2<Integer, Double>(key, 1.0);
        }
      }
    );

    System.out.println("----------rank----------");
    printMap(rank.collectAsMap());

    for (int k = 0; k < iter; k++) {

      JavaPairRDD<Integer, Tuple2<List<Integer>, Double>> join = graph.join(rank);

      System.out.println("----------join----------");
      Map<Integer, Tuple2<List<Integer>, Double>> joinMap = join.collectAsMap();

      for (Map.Entry<Integer, Tuple2<List<Integer>, Double>> e : joinMap.entrySet()) {
        System.out.println(e.getKey() + ":" + e.getValue()._1 + ":" + e.getValue()._2);
      }

      JavaRDD<Tuple2<List<Integer>, Double>> contrib = join.values();

      System.out.println("----------contrib----------");
      List<Tuple2<List<Integer>, Double>> contribList = contrib.collect();

      for (Tuple2<List<Integer>, Double> t : contribList) {
        System.out.println(t._1 + ":" + t._2);
      }

      JavaRDD<Tuple2<Integer, Double>> contribFlat = contrib.flatMap(
        new FlatMapFunction<Tuple2<List<Integer>, Double>, Tuple2<Integer, Double>>() {
          @Override
          public Iterable<Tuple2<Integer, Double>> call(Tuple2<List<Integer>, Double> tuple) {
            double rank = tuple._2;
            List<Integer> recevs = tuple._1;
            double addon = rank / recevs.size();

            List<Tuple2<Integer, Double>> result = new ArrayList<>();

            for (Integer recev : recevs) {
              result.add(new Tuple2<Integer, Double>(recev, addon));
            }

	    return result;
          }
        }
      );

      System.out.println("----------contrib flat----------");
      List<Tuple2<Integer, Double>> contribFlatList = contribFlat.collect();
      for (Tuple2<Integer, Double> t : contribFlatList) {
        System.out.println(t._1 + ":" + t._2);
      }

      JavaPairRDD<Integer, Double> contribFlatPair = contribFlat.mapToPair(
        new PairFunction<Tuple2<Integer, Double>, Integer, Double>() {
          @Override
          public Tuple2<Integer, Double> call(Tuple2<Integer, Double> t) {
            return t;
          }
        }
      );

      //System.out.println("----------contrib flat pair----------");
      //printMap(contribFlatPair.collectAsMap());    

      JavaPairRDD<Integer, Double> contribReduced = contribFlatPair.reduceByKey(
        new Function2<Double, Double, Double>() {
          @Override
          public Double call(Double d1, Double d2) {
            return d1 + d2;
          }
        }
      );

      System.out.println("----------contrib reduced----------");
      printMap(contribReduced.collectAsMap());

      rank = contribReduced.mapValues(
        new Function<Double, Double>() {
          @Override
          public Double call(Double d) {
            return 0.15 + 0.85 * d;
          }
        }
      );

      rank.cache();

      System.out.println("----------rank----------");
      printMap(rank.collectAsMap());

      JavaPairRDD<Double, Integer> rankSorted = rank.mapToPair(
        new PairFunction<Tuple2<Integer, Double>, Double, Integer>() {
          @Override
          public Tuple2<Double, Integer> call(Tuple2<Integer, Double> t) {
            return new Tuple2<Double, Integer>(t._2, t._1);
          }
        }
      );

      rankSorted = rankSorted.sortByKey(false);

      JavaRDD<Integer> sorted = rankSorted.values();
      System.out.println("----------rank sorted----------");
      List<Integer> sortedResult = sorted.collect();
      for (Integer i : sortedResult) {
        System.out.println(i.intValue());
      }
    }
  }

  public static <K, V> void printMap(Map<K, V> map) {
    for (Map.Entry<K, V> e : map.entrySet()) {
      System.out.println(e.getKey() + ":" + e.getValue());
    }
  }
}
