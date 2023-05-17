package learning.bigdata.spark.core.framework.service;

import learning.bigdata.spark.core.framework.common.TService;
import learning.bigdata.spark.core.framework.dao.WordCountDao;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountService implements TService {

    private WordCountDao wordCountDao = new WordCountDao();

    public JavaPairRDD<String, Integer> dataAnalysis(){

        JavaRDD<String> lines = wordCountDao.readFile("spark-core/src/main/resources/word.txt");
        // 扁平化处理
        JavaRDD<String> words = lines.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator());

        // words.foreach(w-> System.out.println(w));
        JavaPairRDD<String, Integer> pairWords = words.mapToPair((String s) -> new Tuple2<>(s, 1));

        // reduce
        JavaPairRDD<String, Integer> result = pairWords.reduceByKey((count1,count2)->count1+count2);

        return result;
    }
}
