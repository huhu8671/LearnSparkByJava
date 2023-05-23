package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RDDOperatorDistinct {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorDistinct")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 9, 1, 2, 3);

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);
        // 底层先变成map，然后reduceByKey进行去重
        JavaRDD<Integer> distinct = javaRDD.distinct();

        distinct.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
