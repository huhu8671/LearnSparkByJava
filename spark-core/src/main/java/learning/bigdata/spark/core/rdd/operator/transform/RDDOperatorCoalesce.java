package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDOperatorCoalesce {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorCoalesce")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1,2,3,4);

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,4);

        JavaRDD<Integer> coalesce = javaRDD.coalesce(2);

        coalesce.saveAsTextFile("spark-core/src/main/resources/output");

        javaSparkContext.stop();
    }
}
