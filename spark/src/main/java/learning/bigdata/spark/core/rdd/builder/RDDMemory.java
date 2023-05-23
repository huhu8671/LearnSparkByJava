package learning.bigdata.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class RDDMemory {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDMemory")
                .setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 从内存中创建数据源
        ArrayList<Object> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        // JAVA没有makeRDD
        JavaRDD<Object> javaRDD = javaSparkContext.parallelize(list);

        javaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
