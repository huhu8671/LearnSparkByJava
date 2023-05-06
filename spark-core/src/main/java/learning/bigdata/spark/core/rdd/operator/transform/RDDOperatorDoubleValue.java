package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RDDOperatorDoubleValue {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorDoubleValue")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list1 = Arrays.asList(1,2,3,4,5,6,7);
        List<Integer> list2 = Arrays.asList(6,7,8,9,0,1,2);

        JavaRDD<Integer> javaRDD1 = javaSparkContext.parallelize(list1);
        JavaRDD<Integer> javaRDD2 = javaSparkContext.parallelize(list2);

//        // 交集
//        javaRDD1.intersection(javaRDD2).collect().forEach(System.out::print);
//        // 并集
//        javaRDD1.union(javaRDD2).collect().forEach(System.out::print);
//        // 差集
//        javaRDD1.subtract(javaRDD2).collect().forEach(System.out::print);
//        // 拉链
//        javaRDD1.zip(javaRDD2).collect().forEach(System.out::print);

        // 交集
        System.out.println(javaRDD1.intersection(javaRDD2).collect());
        // 并集
        System.out.println(javaRDD1.union(javaRDD2).collect());
        // 差集
        System.out.println(javaRDD1.subtract(javaRDD2).collect());
        // 拉链
        System.out.println(javaRDD1.zip(javaRDD2).collect());

        javaSparkContext.stop();
    }
}
