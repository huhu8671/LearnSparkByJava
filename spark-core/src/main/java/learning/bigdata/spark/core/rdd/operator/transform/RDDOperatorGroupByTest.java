package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RDDOperatorGroupByTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorGroupByTest")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> strings = Arrays.asList("Hello", "Spark", "Hello", "Flink");
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(strings);

        JavaPairRDD<Character, Iterable<String>> characterIterableJavaPairRDD = javaRDD.groupBy(a -> a.charAt(0));

        characterIterableJavaPairRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
