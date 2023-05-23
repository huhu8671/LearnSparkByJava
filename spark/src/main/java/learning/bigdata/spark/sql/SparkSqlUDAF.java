package learning.bigdata.spark.sql;

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

public class SparkSqlUDAF {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlUDAF");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        // spark/src/main/resources/user.json
        Dataset<Row> ds = session.read().json("spark/src/main/resources/user.json");
        ds.createOrReplaceTempView("user");

        session.udf().register("MyAvgFunc",new MyAvgFunc());
        ds.createOrReplaceTempView("user");
        session.sql("select MyAvgFunc(age) as avg_age from user").show();

        session.close();
        javaSparkContext.stop();
    }

    /*自定义聚合函数UDAF*/
    static class MyAvgFunc extends UserDefinedAggregateFunction {
        // 定义输入数据的结构类型
        @Override
        public StructType inputSchema() {
            return new StructType().add("age",DataTypes.IntegerType);
        }

        // 定义缓冲区数据的结构类型
        @Override
        public StructType bufferSchema() {
            return new StructType().add("total",DataTypes.IntegerType).
                    add("count",DataTypes.IntegerType);
        }

        // 定义聚合函数的返回类型
        @Override
        public DataType dataType() {
            return DataTypes.IntegerType;
        }

        // 定义聚合函数是否确定性的
        @Override
        public boolean deterministic() {
            return true;
        }

        // 初始化缓冲区的值
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            // 根据i的位置与上面对应
            buffer.update(0,0);
            buffer.update(1,0);
        }

        // 根据输入数据更新缓冲区的值
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                int total = buffer.getInt(0) + input.getInt(0);
                int count = buffer.getInt(1) + 1;
                buffer.update(0, total);
                buffer.update(1, count);
            }
        }

        // 合并两个缓冲区的值
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            int total = buffer1.getInt(0) + buffer2.getInt(0);
            int count = buffer1.getInt(1) + buffer2.getInt(1);
            buffer1.update(0, total);
            buffer1.update(1, count);
        }

        // 计算并返回最终的聚合结果
        @Override
        public Object evaluate(Row buffer) {
            int total = buffer.getInt(0);
            int count = buffer.getInt(1);
            if (count != 0) {
                return total / count;
            } else {
                return null;
            }
        }
    }
 }
