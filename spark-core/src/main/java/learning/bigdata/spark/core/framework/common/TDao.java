package learning.bigdata.spark.core.framework.common;

import learning.bigdata.spark.core.framework.utils.EnvUtil;
import org.apache.spark.api.java.JavaRDD;

public abstract class TDao {

    public JavaRDD<String> readFile(String path){
        // 读文件
        return EnvUtil.take().textFile(path);
    }

}
