package learning.bigdata.spark.core.rdd.operator.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 同一省份广告前三名
 */
public class RDDOperatorTrasnsformTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorTrasnsformTest")
                .setMaster("local[*]");
        JavaSparkContext javaContext = new JavaSparkContext(sparkConf);
        // 数据结构：时间戳 省份 城市 用户 广告
        JavaRDD<String> javaRDD = javaContext.textFile("spark-core/src/main/resources/agent.log");

        javaRDD.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() { // 过滤数据形成k-v
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String taxPeriod) throws Exception {
                String[] s = taxPeriod.split(" ");
                return new Tuple2<>(new Tuple2<>(s[1], s[4]), 1);
            }
        }).reduceByKey((x,y)->x+y).mapToPair( // 先聚合，再重构k-v
                new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<String,Integer>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                        return new Tuple2<>(tuple2IntegerTuple2._1._1,new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
                    }
                }
        ).groupByKey().mapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() { // 先分区，同一key下的数据在一个迭代器里，统一处理
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {
                List<Tuple2<String, Integer>> adClickList = new ArrayList<>();
                for (Tuple2<String, Integer> adClick : v1) {
                    adClickList.add(adClick);
                }
                // 按照点击量降序排列
                adClickList.sort(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                        return t2._2 - t1._2;
                    }
                });
                // 取前三个广告
                List<Tuple2<String, Integer>> top3AdClickList = new ArrayList<>();
                int count = 0;
                for (Tuple2<String, Integer> adClick : adClickList) {
                    top3AdClickList.add(adClick);
                    count++;
                    if (count >= 3) {
                        break;
                    }
                }
                return top3AdClickList;
            }
        }).collect().forEach(System.out::println);

        javaContext.stop();
    }
}
