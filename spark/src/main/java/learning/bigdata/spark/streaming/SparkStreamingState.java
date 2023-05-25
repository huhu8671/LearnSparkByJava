package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkStreamingState {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingWC").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期
        ssc.checkpoint("cp");
        /**
         * updateStateByKey()
         * 1. 第一个值表示相同的key的value数据(List保存)
         * 2. 第二个值表示缓存区相同的key的value数据
         */
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(a -> new Tuple2<>(a,1)).updateStateByKey(
                        (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (v1, v2) -> {
                            Integer sum = v2.orElse(0);
                            for (Integer v:v1){
                                sum += v;
                            }
                            return Optional.of(sum);
                        }
                ).print();


        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }
}
