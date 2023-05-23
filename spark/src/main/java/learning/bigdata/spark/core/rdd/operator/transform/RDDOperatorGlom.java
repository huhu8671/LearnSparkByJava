package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class RDDOperatorGlom {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorGlom")
                .setMaster("local[4]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);

        JavaRDD<List<Integer>> glom = javaRDD.glom();

        // 根据输出分区数量输出结果个数
        glom.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
