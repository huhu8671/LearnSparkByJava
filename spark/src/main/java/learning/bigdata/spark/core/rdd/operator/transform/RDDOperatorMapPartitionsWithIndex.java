package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RDDOperatorMapPartitionsWithIndex {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorMapPartitionsWithIndex")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list,2);

        // 主要Java版本要传入两个参数
        // preservesPartitioning，该参数指定转换操作是否保留原有的分区方式
        JavaRDD<Integer> integerJavaRDD = javaRDD.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
                    @Override
                    public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                        List<Integer> result = new ArrayList<>();
                        while (v2.hasNext()) {
                            Integer next = v2.next();
                            result.add(next * v1);
                        }
                        return result.iterator();
                    }
                }, false
        );

        integerJavaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
