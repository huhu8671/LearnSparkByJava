package learning.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class SparkStreamingUDSource {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingUDSource").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(3));// Duration 表示采集周期

        // 获取端口数据
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(new MySource());
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(a -> new Tuple2<>(a,1)).reduceByKey((x,y) -> x+y).print();

        ssc.start();// 启动采集器
        ssc.awaitTermination();// 等待采集器关闭
    }

    public static class MySource extends Receiver<String>{

        private boolean flag = true;

        public MySource() {
            super(StorageLevel.MEMORY_ONLY());
        }

        @Override
        public void onStart() {
            new Thread(() -> {
                while(flag){
                    String i = "采集数据为" +  new Random().nextInt(10);
                    store(i);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        @Override
        public void onStop() {

        }
    }
}
