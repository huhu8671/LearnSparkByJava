package learning.bigdata.spark.core.rdd.operator.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

public class RDDOperatorFlatMap {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDOperatorFlatMap")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        ArrayList<ArrayList<Integer>> list = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ArrayList<Integer> l = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                l.add(j);
            }
            list.add(l);
        }
        JavaRDD<ArrayList<Integer>> javaRDD = javaSparkContext.parallelize(list);

        JavaRDD<Integer> integerJavaRDD = javaRDD.flatMap(
                new FlatMapFunction<ArrayList<Integer>, Integer>() {
                    @Override
                    public Iterator<Integer> call(ArrayList<Integer> integers) throws Exception {
                        ArrayList<Integer> list1 = new ArrayList<>();
                        for (Integer i : integers) {
                            list1.add(i * 2);
                        }
                        return list1.iterator();
                    }
                }
        );

        integerJavaRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
