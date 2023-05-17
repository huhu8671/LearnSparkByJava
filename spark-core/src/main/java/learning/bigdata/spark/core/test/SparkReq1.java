package learning.bigdata.spark.core.test;

import learning.bigdata.spark.core.bean.ClickOrderOfferSort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * reduceByKey实现需求一
 */
public class SparkReq1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkReq1").setMaster("local[4]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD = javaSparkContext.textFile("spark-core/src/main/resources/user_visit_action.txt");
        // (品类id, new JavaBean implements Ordered<JavaBean>, Serializable)
        // 使用java bean实现两个接口，实现了Spark的SecondarySortKey二次排序
        JavaRDD<Tuple2<String, ClickOrderOfferSort>> tuple2JavaRDD = javaRDD.flatMap(new FlatMapFunction<String, Tuple2<String, ClickOrderOfferSort>>() {
            @Override
            public Iterator<Tuple2<String, ClickOrderOfferSort>> call(String v1) throws Exception {
                String[] s = v1.split("_");
                ArrayList<Tuple2<String, ClickOrderOfferSort>> list = new ArrayList<>();
                if (!s[6].equals("-1")) {
                    // 点击
                    list.add(new Tuple2<>(s[6], new ClickOrderOfferSort(1,0,0)));
                } else if (!s[8].equals("null")) {
                    // 下单
                    String[] splits = s[8].split(",");
                    for (String split : splits) {
                        list.add(new Tuple2<>(split, new ClickOrderOfferSort(0,1,0)));
                    }
                } else if (!s[10].equals("null")) {
                    // 支付
                    String[] splits = s[10].split(",");
                    for (String split : splits) {
                        list.add(new Tuple2<>(split, new ClickOrderOfferSort(0,0,1)));
                    }
                }

                return list.iterator();
            }
        });
        JavaPairRDD<String, ClickOrderOfferSort> javaPairRDD = JavaPairRDD.fromJavaRDD(tuple2JavaRDD).reduceByKey(
                (x, y) -> new ClickOrderOfferSort(x.getClicks()+ y.getClicks(),
                        x.getOrders()+y.getOrders(), x.getPayments()+ y.getPayments()));

        JavaRDD<Tuple2<String, ClickOrderOfferSort>> map = javaPairRDD.map(new Function<Tuple2<String, ClickOrderOfferSort>, Tuple2<String, ClickOrderOfferSort>>() {
            @Override
            public Tuple2<String, ClickOrderOfferSort> call(Tuple2<String, ClickOrderOfferSort> v1) throws Exception {
                return v1;
            }
        });

        map.sortBy(new Function<Tuple2<String, ClickOrderOfferSort>, ClickOrderOfferSort>() {
            @Override
            public ClickOrderOfferSort call(Tuple2<String, ClickOrderOfferSort> v1) throws Exception {
                return v1._2;
            }
        },false,2).take(10).forEach(
                new Consumer<Tuple2<String, ClickOrderOfferSort>>() {
                    @Override
                    public void accept(Tuple2<String, ClickOrderOfferSort> stringClickOrderOfferSortTuple2) {
                        System.out.println(stringClickOrderOfferSortTuple2._1+":"+stringClickOrderOfferSortTuple2._2.toString());
                    }
                }
        );
    }
}
