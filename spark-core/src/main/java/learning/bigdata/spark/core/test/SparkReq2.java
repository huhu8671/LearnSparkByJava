package learning.bigdata.spark.core.test;


import learning.bigdata.spark.core.bean.GoodsInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

/**
 * 累加器实现需求一
 */
public class SparkReq2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkReq2").setMaster("local[4]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD = javaSparkContext.textFile("spark-core/src/main/resources/user_visit_action.txt");
        HotCategorySortAcc myAcc = new HotCategorySortAcc();
        javaSparkContext.sc().register(myAcc,"HotCategory");
        javaRDD.foreach(
                new VoidFunction<String>() {
                    @Override
                    public void call(String v) throws Exception {
                        String[] s = v.split("_");
                        if (!s[6].equals("-1")) {
                            // 点击
                            myAcc.add(new Tuple2<>(s[6],"click"));
                        } else if (!s[8].equals("null")) {
                            // 下单
                            String[] splits = s[8].split(",");
                            for (String split : splits) {
                                myAcc.add(new Tuple2<>(split,"order"));
                            }
                        } else if (!s[10].equals("null")) {
                            // 支付
                            String[] splits = s[10].split(",");
                            for (String split : splits) {
                                myAcc.add(new Tuple2<>(split,"payment"));
                            }
                        }
                    }
                }
        );
        HashMap<String, GoodsInfo> value = myAcc.value();
        ArrayList<Map.Entry<String, GoodsInfo>> list = new ArrayList<>(value.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, GoodsInfo>>() {
            @Override
            public int compare(Map.Entry<String, GoodsInfo> o1, Map.Entry<String, GoodsInfo> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        for (int i = 0; i < 10; i++) {
            Map.Entry<String, GoodsInfo> entry = list.get(i);
            System.out.println(entry.getKey()+ " : "+entry.getValue().toString());
        }
    }

    public static class HotCategorySortAcc extends AccumulatorV2<Tuple2<String,String>, HashMap<String, GoodsInfo>>{

        private HashMap<String,GoodsInfo> map = new HashMap<>();

        @Override
        public boolean isZero() {
            return map.isEmpty();
        }

        @Override
        public AccumulatorV2<Tuple2<String, String>, HashMap<String, GoodsInfo>> copy() {
            return new HotCategorySortAcc();
        }

        @Override
        public void reset() {
            map.clear();
        }

        @Override
        public void add(Tuple2<String, String> v) {
            GoodsInfo orDefault = map.getOrDefault(v._1, new GoodsInfo(0, 0, 0));
            String s = v._2;
            if (s.equals("click")){
                orDefault.setClicks(orDefault.getClicks()+1);
            }else if (s.equals("order")){
                orDefault.setOrders(orDefault.getOrders()+1);
            }else if (s.equals("payment")){
                orDefault.setPayments(orDefault.getPayments()+1);
            }
            map.put(v._1,orDefault);
        }

        @Override
        public void merge(AccumulatorV2<Tuple2<String, String>, HashMap<String, GoodsInfo>> other) {
            HashMap<String,GoodsInfo> map1 = this.map;
            HashMap<String,GoodsInfo> map2 = other.value();
            for (Map.Entry<String,GoodsInfo> entry:map2.entrySet()){
                GoodsInfo orDefault = map1.getOrDefault(entry.getKey(), new GoodsInfo(0, 0, 0));
                orDefault.setClicks(entry.getValue().getClicks() + orDefault.getClicks());
                orDefault.setOrders(entry.getValue().getOrders() + orDefault.getOrders());
                orDefault.setPayments(entry.getValue().getPayments() + orDefault.getPayments());
                map1.put(entry.getKey(),orDefault);
            }
        }

        @Override
        public HashMap<String, GoodsInfo> value() {
            return map;
        }
    }
}
