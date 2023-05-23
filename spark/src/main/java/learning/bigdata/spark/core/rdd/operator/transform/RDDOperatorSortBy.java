package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDOperatorSortBy {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorSortBy")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = Arrays.asList(1,2,3,4,7,6,8,9,5);

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,4);

        /**
         * 1. function规则
         * 2. 升序还是降序，默认升序
         * 3. 分区数
         */
        JavaRDD<Integer> sort = javaRDD.sortBy(a -> a,true,2);

        //sort.collect().forEach(System.out::println);

        sort.saveAsTextFile("spark-core/src/main/resources/output");

        javaSparkContext.stop();
    }
}
