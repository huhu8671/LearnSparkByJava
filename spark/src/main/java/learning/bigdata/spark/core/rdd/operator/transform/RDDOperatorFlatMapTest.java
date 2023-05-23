package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class RDDOperatorFlatMapTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorFlatMapTest")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<String> list = new ArrayList<String>();
        list.add("hello word");
        list.add("hello spark");
        list.add("hello flink");
        list.add("hello mr");

        JavaRDD<String> javaRDD = javaSparkContext.parallelize(list);
        // 读取一个然后根据空拆分扁平化
        JavaRDD<String> integerJavaRDD = javaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        integerJavaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
