package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class RDDOperatorSample {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorSample")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);


        /**
         * 三个参数：1. 抽取后是否放回
         *         2. 抽取不放回的场合，每条数据被抽取的概率；抽取放回的场合，表示数据源中每条数据被抽取的次数
         *         3. 抽取数据时随机算法的种子，相同种子返回的值是相同的,不传递就是随机的
         */
        javaRDD.sample(true,0.4,1).collect().forEach(System.out::print);

        javaSparkContext.stop();
    }
}
