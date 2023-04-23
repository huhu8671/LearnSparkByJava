import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;

public class SimpleApp {
    public static void main(String[] args) {
        //lamda表达式
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_demo_java");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("G:\\找工作\\spark-test\\spark-core\\src\\main\\resources\\word.txt");
//        List<String> data = Arrays.asList("1", "2", "3", "4", "1"); // 从内存读
//        JavaRDD<String> lines = sc.parallelize(data);

        JavaRDD<String> words = lines.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator());
        words.foreach(w-> System.out.println(w));
        JavaPairRDD<String, Integer> pairWords = words.mapToPair((String s) -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> result = pairWords.reduceByKey(Integer::sum);

        result.foreach(wordCount -> System.out.println(wordCount._1() + ": " + wordCount._2()));


        sc.stop();
    }
}
