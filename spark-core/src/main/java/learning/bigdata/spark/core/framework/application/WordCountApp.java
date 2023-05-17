package learning.bigdata.spark.core.framework.application;

import learning.bigdata.spark.core.framework.common.TApplication;
import learning.bigdata.spark.core.framework.controller.WordCountController;


public class WordCountApp extends TApplication {
    public static void main(String[] args) {
        WordCountApp wordCountApp = new WordCountApp();
        wordCountApp.start("local[*]","WordCountApp",()->{
            WordCountController wordCountController = new WordCountController();
            wordCountController.execute();
        });
    }
}
