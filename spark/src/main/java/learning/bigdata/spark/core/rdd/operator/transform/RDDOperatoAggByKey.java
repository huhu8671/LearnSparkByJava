package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDOperatoAggByKey {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatoAggByKey")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a","a","b","b","c","c","d","d","e");
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(list,2);
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(a -> new Tuple2<>(a, 1));

        // 第一个func是分区内计算规则，第二个func是分区间计算规则
        JavaPairRDD<String, Integer> partition = javaPairRDD.aggregateByKey(0, Integer::sum, Integer::max);

        partition.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
