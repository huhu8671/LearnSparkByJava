package learning.bigdata.spark.core.rdd.operator.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDPersist {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDDPersist")
                .setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //javaSparkContext.setCheckpointDir("spark-core/src/main/resources/cp/");

        JavaRDD<String> lines = javaSparkContext.textFile("spark/src/main/resources/word.txt");

        // 扁平化处理
        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String v1) throws Exception {
                        ArrayList<String> list = new ArrayList<>();
                        String[] ss = v1.split(" ");
                        for(String s:ss){
                            list.add(s);
                        }
                        System.out.println("********");
                        return list.iterator();
                    }
                }
        );
        // map
        JavaPairRDD<String, Integer> pairWords = words.mapToPair((String s) -> new Tuple2<>(s, 1));
//        // 持久化到内存
//        pairWords.cache();
//        pairWords.persist(StorageLevel.MEMORY_ONLY());
        pairWords.checkpoint();
        // reduce
        JavaPairRDD<String, Integer> result = pairWords.reduceByKey((count1,count2)->count1+count2);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = pairWords.groupByKey();

        // 输出
        result.foreach(wordCount -> System.out.println(wordCount._1() + ": " + wordCount._2()));
        stringIterableJavaPairRDD.collect().forEach(System.out::println);

        javaSparkContext.stop();
    }
}
