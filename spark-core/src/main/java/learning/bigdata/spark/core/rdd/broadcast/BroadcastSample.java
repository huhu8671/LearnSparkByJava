package learning.bigdata.spark.core.rdd.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class BroadcastSample {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("AccSample");
        conf.setMaster("local[2]");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 3)
        );
        JavaRDD<Tuple2<String, Integer>> javaRDD = sc.parallelize(list);
        HashMap<String,Integer> mapInner = new HashMap<>();
        mapInner.put("a",4);
        mapInner.put("b",5);
        mapInner.put("c",6);

        Broadcast<HashMap<String, Integer>> broadcast = sc.broadcast(mapInner);
        javaRDD.map(new Function<Tuple2<String, Integer>, Tuple2<String,List<Integer>>>() {
            @Override
            public Tuple2<String,List<Integer>> call(Tuple2<String, Integer> v1) throws Exception {
                // 访问广播变量
                Integer orDefault = broadcast.getValue().getOrDefault(v1._1, 0);
                return new Tuple2<>(v1._1,Arrays.asList(v1._2,orDefault));
            }
        }).collect().forEach(System.out::println);

        sc.stop();
    }
}
