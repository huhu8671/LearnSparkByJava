package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingStop {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingStop").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期

        // 获取端口数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(a -> new Tuple2<>(a,1)).reduceByKey((x,y) -> x+y).print();

        ssc.start();// 启动采集器
        new Thread(
                ()->{
                    // 优雅的关闭
                    // 计算节点部在接收新的数据，而是将现有的数据处理完毕，然后关闭
                    // 触发的flag可以放到第三方的存储里（Mysql/Redis/ZK/HDFS）
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(ssc.getState()==StreamingContextState.ACTIVE){
                        ssc.stop(true,true);
                    }
                    System.exit(0);
                }
        ).start();
        ssc.awaitTermination();// 等待采集器关闭

    }
}
