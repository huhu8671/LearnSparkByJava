package learning.bigdata.spark.core.framework.common;

import learning.bigdata.spark.core.framework.utils.EnvUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class TApplication {
    public void start(String master, String app, Runnable op) {
        SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(app);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        EnvUtil.put(sc);

        try {
            op.run();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        sc.stop();
        EnvUtil.clear();
    }
}
