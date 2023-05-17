package learning.bigdata.spark.core.bean;

import java.io.Serializable;

public class GoodsInfo implements Comparable<GoodsInfo>, Serializable {
    int clicks;
    int orders;
    int payments;

    public GoodsInfo(int clicks, int orders, int payments) {
        this.clicks = clicks;
        this.orders = orders;
        this.payments = payments;
    }

    public int getClicks() {
        return clicks;
    }

    public void setClicks(int clicks) {
        this.clicks = clicks;
    }

    public int getOrders() {
        return orders;
    }

    public void setOrders(int orders) {
        this.orders = orders;
    }

    public int getPayments() {
        return payments;
    }

    public void setPayments(int payments) {
        this.payments = payments;
    }

    @Override
    public int compareTo(GoodsInfo o) {
        if (this.getClicks()>o.getClicks()){
            return 1;
        }else if (this.getClicks()==o.getClicks()&&this.getOrders()>this.getOrders()){
            return 1;
        }else if (this.getClicks()==o.getClicks()&&
                this.getOrders()==this.getOrders()&&this.getPayments()>o.getPayments()){
            return 1;
        }else{
            return -1;
        }
    }

    @Override
    public String toString() {
        return "GoodsInfo{" +
                "clicks=" + clicks +
                ", orders=" + orders +
                ", payments=" + payments +
                '}';
    }
}
