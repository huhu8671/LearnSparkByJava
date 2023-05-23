package learning.bigdata.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;

public class RDDMemoryPar {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        // setMaster("local")表示并行度为一,setMaster("local[*]")表示使用最大核数,setMaster("local[3]")表示用3核
        sparkConf.setAppName("RDDMemory")
                .setMaster("local[3]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // 从内存中创建数据源
        ArrayList<Object> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        // 第二个参数代表分区
        // 默认并行度根据配置参数和默认值比较取得，默认值是当前可用的最大核数
        JavaRDD<Object> javaRDD = javaSparkContext.parallelize(list);

        // 将数据保存成分区文件
        javaRDD.saveAsTextFile("spark/src/main/resources/output");

        javaSparkContext.stop();
    }
}
