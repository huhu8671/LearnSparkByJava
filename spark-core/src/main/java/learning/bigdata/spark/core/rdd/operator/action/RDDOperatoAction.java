package learning.bigdata.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class RDDOperatoAction {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatoReduce")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> list = Arrays.asList("a","b","c","d","e");
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(list,2);

//        System.out.println(javaRDD.reduce((x, y) -> x + y));
//
//        System.out.println(javaRDD.collect());
//
//        System.out.println(javaRDD.count());
//
//        System.out.println(javaRDD.first());
//
//        System.out.println(javaRDD.take(2));
//
//        System.out.println(javaRDD.takeOrdered(3));

//        System.out.println(javaRDD.aggregate("",String::concat,String::concat));
//        System.out.println(javaRDD.fold("",String::concat));

//        System.out.println(javaRDD.countByValue());

//        javaRDD.foreach(System.out::println);// 不能这么写，因为没有进行序列化
        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String taxPeriod) throws Exception {
                System.out.println(taxPeriod);
            }
        });

        javaSparkContext.stop();
    }
}
