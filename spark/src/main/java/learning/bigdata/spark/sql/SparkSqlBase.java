package learning.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;

public class SparkSqlBase {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlBase");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        // spark/src/main/resources/user.json
        Dataset<Row> ds = session.read().json("spark/src/main/resources/user.json");
        /*使用sql实现*/
//        ds.createOrReplaceTempView("user");
//        session.sql("select * from user");
        /*使用api实现*/
        ds.select("age").show();

        ds.toDF().show();
        ds.toJavaRDD().collect().forEach(System.out::println);


        session.close();
        javaSparkContext.stop();
    }
}
