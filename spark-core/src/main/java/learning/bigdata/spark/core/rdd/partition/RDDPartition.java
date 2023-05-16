package learning.bigdata.spark.core.rdd.partition;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDPartition {
    public static void main(String[] args) {
        // 环境配置
        SparkConf conf = new SparkConf();
        conf.setAppName("RDDPartition");
        conf.setMaster("local");
        // 读入环境
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> tuple2s = Arrays.asList(
                new Tuple2<>("NBA", "XXXX"),
                new Tuple2<>("NBA", "XXXX"),
                new Tuple2<>("CBA", "XXXX"),
                new Tuple2<>("NBA", "XXXX"),
                new Tuple2<>("CBA", "XXXX"),
                new Tuple2<>("WNBA", "XXXX")
        );
        JavaRDD<Tuple2<String, String>> javaRDD = sc.parallelize(tuple2s);
        JavaPairRDD<String, String> javaPairRDD = JavaPairRDD.fromJavaRDD(javaRDD);

        javaPairRDD.partitionBy(new MyPatitioner()).collect().forEach(System.out::println);
        
        sc.stop();
    }

    /**
     * 自定义分区器
     */
    public static class MyPatitioner extends Partitioner {
        // 分区数量
        @Override
        public int numPartitions() {
            return 3;
        }

        // 根据数据key返回数据的分区索引（从0开始）
        @Override
        public int getPartition(Object key) {
            if ("NBA".equals(key)) return 0;
            else if ("WNBA".equals(key)) return 1;
            else return 2;
        }
    }
}
