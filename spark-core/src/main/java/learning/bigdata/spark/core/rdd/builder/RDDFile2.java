package learning.bigdata.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFile2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDFile")
                .setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);


        // 注意是minPartitions,最小分区数量
        // 读取文件底层使用的是hadoop的读取方式
        // 分区数量的计算方式：
        //      totalSize = 7 字节数 算上换行符
        //      goalSize = 7/2(分区数) = 3(byte)
        //      7/3 = 2....1 余出来的1有两种处理方式：
        //      1. 如果余出来的大于分区大小的0.1倍，则新建一个分区;
        //      2. 如果小于则不创建新分区而是追加到某一分区里
        JavaRDD<String> stringJavaRDD = javaSparkContext.
                textFile("spark-core/src/main/resources/word.txt", 2);

        // 将数据保存成分区文件
        stringJavaRDD.saveAsTextFile("spark-core/src/main/resources/output");

        javaSparkContext.stop();
    }
}
