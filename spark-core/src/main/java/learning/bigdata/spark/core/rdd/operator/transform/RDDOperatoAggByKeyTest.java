package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDOperatoAggByKeyTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatoAggByKeyTest")
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
        // arg1：学科成绩总分和学生人数
        // fun1：对于每个分区内的数据进行聚合的函数，将之前的Tuple2<Double, Integer>累加得到一个新的Tuple2<Double, Integer>
        // fun2: 将不同分区内的结果进行聚合的函数，同样将两个Tuple2<Double, Integer>累加得到一个新的Tuple2<Double, Integer>。
        JavaPairRDD<String, Tuple2<Double, Integer>> scoreSumCountPairRDD = scorePairRDD.aggregateByKey(
                new Tuple2<>(0.0, 0),
                (Tuple2<Double, Integer> acc, Double score) -> new Tuple2<>(acc._1() + score, acc._2() + 1),// a(0.0,0) + a(1.0) => a(1.0,1)
                (Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) -> new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2())
        );

        JavaPairRDD<String, Double> avgScorePairRDD = scoreSumCountPairRDD.mapValues(
                (Tuple2<Double, Integer> sumCount) -> sumCount._1() / sumCount._2() // a(9.0,3)=>9.0/3
        );

        avgScorePairRDD.foreach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));

        javaSparkContext.stop();
    }
}
