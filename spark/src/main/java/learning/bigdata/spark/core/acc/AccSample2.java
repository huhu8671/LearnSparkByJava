package learning.bigdata.spark.core.acc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccSample2 {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("AccSample2");
        conf.setMaster("local[2]");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 转换成SparkContext,获取系统累加器
        LongAccumulator sum = sc.sc().longAccumulator();

        List<Integer> list = Arrays.asList(
                1,2,3,4
        );

        JavaRDD<Integer> javaRDD = sc.parallelize(list);

        //javaRDD.cache();
        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
                //System.out.println(sum.value());
            }
        });
        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
                //System.out.println(sum.value());
            }
        });

        System.out.println(sum.value());
        sc.stop();
    }
}
