package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingWC {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingWC").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期

        // 获取端口数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(a -> new Tuple2<>(a,1)).reduceByKey((x,y) -> x+y).print();

        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }
}
