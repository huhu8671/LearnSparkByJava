package learning.bigdata.spark.core.framework.controller;

import learning.bigdata.spark.core.framework.common.TController;
import learning.bigdata.spark.core.framework.service.WordCountService;
import org.apache.spark.api.java.JavaPairRDD;

public class WordCountController implements TController {

    private WordCountService wordCountService = new WordCountService();

    public void execute(){

        JavaPairRDD<String, Integer> result = wordCountService.dataAnalysis();

        // 输出
        result.foreach(wordCount -> System.out.println(wordCount._1() + ": " + wordCount._2()));
    }
}
