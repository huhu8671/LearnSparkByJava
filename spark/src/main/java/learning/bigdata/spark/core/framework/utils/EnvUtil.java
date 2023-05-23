package learning.bigdata.spark.core.framework.utils;

import org.apache.spark.api.java.JavaSparkContext;

public class EnvUtil {

    private static ThreadLocal<JavaSparkContext> local = new ThreadLocal<>();

    public static void put(JavaSparkContext sc){
        local.set(sc);
    }

    public static JavaSparkContext take(){
        return local.get();
    }

    public static void clear(){
        local.remove();
    }
}
