package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;

public class RDDOperatorMapTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorMapTest")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("spark-core/src/main/resources/log");
        // 小功能
        JavaRDD<String> map = stringJavaRDD.map(
                new Function<String, String>() {
                    @Override
                    public String call(String v1) throws Exception {
                        String[] s = v1.split(" ");
                        return s[s.length-1];
                    }
                }
        );

        map.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
