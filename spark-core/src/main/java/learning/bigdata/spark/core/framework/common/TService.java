package learning.bigdata.spark.core.framework.common;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface TService {

    JavaPairRDD<String, Integer> dataAnalysis();
}
