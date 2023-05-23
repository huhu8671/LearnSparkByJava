package learning.bigdata.spark.core.acc;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AccSample {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("AccSample");
        conf.setMaster("local[2]");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(
                1,2,3,4
        );
        JavaRDD<Integer> javaRDD = sc.parallelize(list);

        final int[] sum = {0};
        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum[0] += integer;
                System.out.println(sum[0]);
            }
        });
        System.out.println(sum[0]);
        sc.stop();
    }
}
