package learning.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class SparkSqlUDAFByAggregator {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlUDAFByAggregator");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        // spark/src/main/resources/user.json
        Dataset<Row> ds = session.read().json("spark/src/main/resources/user.json");
        ds.createOrReplaceTempView("user");

        session.udf().register("MyAvgFunc",functions.udaf(new MyAvgUDAF(),Encoders.DOUBLE()));
        ds.createOrReplaceTempView("user");
        session.sql("select MyAvgFunc(age) as avg_age from user").show();

        session.close();
        javaSparkContext.stop();
    }

    // 缓冲区类型
    public static class Buff implements Serializable {
        double sum;
        double count;

        public Buff(double sum,double count){
            this.sum = sum;
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public double getCount() {
            return count;
        }

        public void setCount(double count) {
            this.count = count;
        }
    }

    /*自定义聚合函数UDAF*/
    public static class MyAvgUDAF extends Aggregator<Double, Buff, Double> {

        // 初始化缓冲区
        @Override
        public Buff zero() {
            return new Buff(0.0,0.0);
        }

        // 根据输入数据更新缓冲区的值
        @Override
        public Buff reduce(Buff b, Double a) {
            b.setSum(b.getSum()+a);
            b.setCount(b.getCount()+1);
            return b;
        }

        // 合并两个缓冲区的值
        @Override
        public Buff merge(Buff b1, Buff b2) {
            b1.setSum(b1.getSum()+b2.getSum());
            b1.setCount(b1.getCount()+b2.getCount());
            return b1;
        }

        // 根据最终的缓冲区计算并返回聚合结果
        @Override
        public Double finish(Buff reduction) {
            return reduction.getSum()/ reduction.getCount();
        }

        // 指定缓冲区数据的编码器
        @Override
        public Encoder<Buff> bufferEncoder() {
            return Encoders.javaSerialization(Buff.class);
        }

        // 指定输出数据的编码器
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
 }
