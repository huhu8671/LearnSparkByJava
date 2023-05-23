package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class RDDOperatoCombineByKey {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatoCombineByKey")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Double> scorePairRDD = JavaPairRDD.fromJavaRDD(javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("a", 1.0),
                new Tuple2<>("a", 2.0),
                new Tuple2<>("b", 3.0),
                new Tuple2<>("b", 4.0),
                new Tuple2<>("b", 5.0),
                new Tuple2<>("a", 6.0)
        )));
        /**
         * arg1: 将相同key的第一个数据进行结构的转换，实现操作
         * arg2: 分区内计算规则
         * arg3: 分区间计算规则
         */
        JavaPairRDD<String, Tuple2<Double, Integer>> scoreSumCountPairRDD = scorePairRDD.combineByKey(
                v -> new Tuple2<>(v,1),
                (Tuple2<Double, Integer> acc, Double score) -> new Tuple2<>(acc._1() + score, acc._2() + 1),
                (Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) -> new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2())
        );

        JavaPairRDD<String, Double> avgScorePairRDD = scoreSumCountPairRDD.mapValues(
                (Tuple2<Double, Integer> sumCount) -> sumCount._1() / sumCount._2() // a(9.0,3)=>9.0/3
        );

        avgScorePairRDD.foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));

        javaSparkContext.stop();
    }
}
