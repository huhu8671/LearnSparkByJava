package learning.bigdata.spark.core.framework.common;

import org.apache.spark.api.java.JavaPairRDD;

public interface TService {

    JavaPairRDD<String, Integer> dataAnalysis();
}
