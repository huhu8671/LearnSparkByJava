package learning.bigdata.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFile1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDFile")
                .setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 路径默认以当前环境的根路径为基准，可以写绝对路径也可以写相对路径
        // 路径可以使用通配符
        // 以文件为单位读取数据，第一个是文件路径，第二个是文件内容
        JavaPairRDD<String, String> javaPairRDD = javaSparkContext.wholeTextFiles("spark/src/main/resources/word.txt");

        javaPairRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
