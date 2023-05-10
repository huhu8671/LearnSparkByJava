package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RDDOperatorPartitionBy {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorPartitionBy")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a","a","b","b","c","c","d","d","e");
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(list,2);
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(a -> new Tuple2<>(a, 1));
        JavaPairRDD<String, Integer> partition = javaPairRDD.partitionBy(new HashPartitioner(2));

        partition.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
