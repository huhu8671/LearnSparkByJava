package learning.bigdata.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class RDDFile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDFile")
                .setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 路径默认以当前环境的根路径为基准，可以写绝对路径也可以写相对路径
        // 路径可以使用通配符
        // 以行为单位读取数据
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("spark/src/main/resources/word.txt");

        stringJavaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
