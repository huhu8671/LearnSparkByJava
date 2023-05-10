package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDOperatoJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatoJoin")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a","b","c","d","e");
        JavaRDD<String> javaRDD1 = javaSparkContext.parallelize(list,2);
        JavaRDD<String> javaRDD2 = javaSparkContext.parallelize(list,2);
        JavaPairRDD<String, Integer> javaPairRDD1 = javaRDD1.mapToPair(a -> new Tuple2<>(a, 1));
        JavaPairRDD<String, Integer> javaPairRDD2 = javaRDD2.mapToPair(a -> new Tuple2<>(a, 2));

        /**
         * leftOuterJoin、rightOuterJoin方法类似就不写了，参考Join的写法
         */
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = javaPairRDD1.join(javaPairRDD2);

        join.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
