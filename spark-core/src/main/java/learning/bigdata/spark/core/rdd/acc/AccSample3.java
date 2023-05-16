package learning.bigdata.spark.core.rdd.acc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccSample3 {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("AccSample3");
        conf.setMaster("local[2]");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList(
                "hello","spark","hello","flink","hello","word","hello","spark"
        );
        JavaRDD<String> javaRDD = sc.parallelize(list);

        // 自定义累加器
        // 创建累加器对象
        MyAcc myAcc = new MyAcc();
        // 向spark进行注册
        sc.sc().register(myAcc);

        javaRDD.foreach(
                new VoidFunction<String>() {
                    @Override
                    public void call(String v) throws Exception {
                        myAcc.add(v);
                    }
                }
        );

        // 打印结果
        System.out.println(myAcc.value());

        sc.stop();
    }

    /**
     * 自定义累加器
     * 1. 继承AccumulatorV2
     * 2. 定义泛型 IN:累加器输入类型;OUT：累加器返回数据类型
     * 3. 重写方法
     */
    public static class MyAcc extends AccumulatorV2<String, HashMap<String,Integer>>{

        private HashMap<String,Integer> map = new HashMap<>();

        // 判断是否为初始状态
        @Override
        public boolean isZero() {
            return map.isEmpty();
        }

        @Override
        public AccumulatorV2<String, HashMap<String, Integer>> copy() {
            return new MyAcc();
        }

        @Override
        public void reset() {
            map.clear();
        }

        @Override
        public void add(String v) {
            map.put(v, map.getOrDefault(v, 0) + 1);
        }
        // 合并driver端返回的累加器
        @Override
        public void merge(AccumulatorV2<String, HashMap<String, Integer>> other) {
            HashMap<String,Integer> map1 = this.map;
            HashMap<String,Integer> map2 = other.value();
            for(Map.Entry<String,Integer> entry:map2.entrySet()){
                map1.put(entry.getKey(),map1.getOrDefault(entry.getKey(),0)+entry.getValue());
            }
        }

        @Override
        public HashMap<String, Integer> value() {
            return map;
        }
    }
}
