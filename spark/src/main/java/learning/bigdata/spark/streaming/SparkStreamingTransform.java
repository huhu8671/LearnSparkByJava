package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingTransform {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingTransform").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期

        // 获取端口数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        //Driver端执行
        JavaDStream<String> transform = lines.transform(
                //Driver端执行,这端代码周期性执行
                rdd-> rdd.map(
                        //Executor端执行
                        a->a
                )
        );
        //Driver端执行
        JavaDStream<String> map = lines.map(
                //Executor端执行
                a -> a
        );

        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }
}
