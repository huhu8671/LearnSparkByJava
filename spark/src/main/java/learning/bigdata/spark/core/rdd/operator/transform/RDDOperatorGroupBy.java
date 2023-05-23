package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;

public class RDDOperatorGroupBy {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorGroupBy")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);

        JavaPairRDD<Integer, Iterable<Integer>> integerIterableJavaPairRDD = javaRDD.groupBy(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1%2;
                    }
                },2
        );

        integerIterableJavaPairRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
