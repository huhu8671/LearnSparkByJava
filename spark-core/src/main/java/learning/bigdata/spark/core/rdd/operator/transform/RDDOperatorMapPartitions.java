package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RDDOperatorMapPartitions {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorMapPartitions")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);

//        JavaRDD<Integer> integerJavaRDD = javaRDD.mapPartitions(
//                new FlatMapFunction<Iterator<Integer>, Integer>() {
//                    @Override
//                    public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
//                        List<Integer> result = new ArrayList<>();
//                        System.out.println(">>>>>>>>>");
//                        while (iterator.hasNext()) {
//                            Integer next = iterator.next();
//                            result.add(next * 2);
//                        }
//                        return result.iterator();
//                    }
//                }
//        );
        // 数据缓存在内存中，如果数据量大会导致内存溢出，lambda写法
        JavaRDD<Integer> integerJavaRDD = javaRDD.mapPartitions(
                (FlatMapFunction<Iterator<Integer>, Integer>) iterator -> {
                    List<Integer> result = new ArrayList<>();
                    System.out.println(">>>>>>>>>");
                    while (iterator.hasNext()) {
                        Integer next = iterator.next();
                        result.add(next * 2);
                    }
                    return result.iterator();
                }
        );

        integerJavaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
