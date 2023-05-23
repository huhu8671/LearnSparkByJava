package learning.bigdata.spark.core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;

public class SimpleApp {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_demo_java");
        conf.setMaster("local");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 读文件
        JavaRDD<String> lines = sc.textFile("spark/src/main/resources/word.txt");

        // 扁平化处理
        JavaRDD<String> words = lines.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator());

        // words.foreach(w-> System.out.println(w));
        // map
        JavaPairRDD<String, Integer> pairWords = words.mapToPair((String s) -> new Tuple2<>(s, 1));

        // reduce
        JavaPairRDD<String, Integer> result = pairWords.reduceByKey((count1,count2)->count1+count2);

        // 输出
        result.foreach(wordCount -> System.out.println(wordCount._1() + ": " + wordCount._2()));

        sc.stop();
    }
}
