package learning.bigdata.spark.sql;

import org.apache.spark.Aggregator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;

public class SparkSqlUDF {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlUDF");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        // spark/src/main/resources/user.json
        Dataset<Row> ds = session.read().json("spark/src/main/resources/user.json");
        ds.createOrReplaceTempView("user");

        session.udf().register("prefixName", new UDF1<String, String>() {
            @Override
            public String call(String v) throws Exception {
                return "Name:" + v;
            }
        }, DataTypes.StringType);


//        实现加前缀
//        session.sql("select concat(\"u-\",username) from user").show();
        session.sql("select prefixName(username) as username from user").show();

        session.close();
        javaSparkContext.stop();
    }
 }
