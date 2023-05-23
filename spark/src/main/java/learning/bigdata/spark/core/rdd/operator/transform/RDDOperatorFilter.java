package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class RDDOperatorFilter {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorFilter")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);

        JavaRDD<Integer> filter = javaRDD.filter(a -> a % 2 == 1);

        filter.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
