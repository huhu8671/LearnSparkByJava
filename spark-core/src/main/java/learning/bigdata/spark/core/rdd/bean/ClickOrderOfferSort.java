package learning.bigdata.spark.core.rdd.bean;

import scala.math.Ordered;

import java.io.Serializable;


// 定义自定义的SecondarySortKey类
public class ClickOrderOfferSort implements Ordered<ClickOrderOfferSort>, Serializable {
    private int clicks;
    private int orders;
    private int payments;

    public ClickOrderOfferSort(int clicks, int orders, int payments) {
        this.clicks = clicks;
        this.orders = orders;
        this.payments = payments;
    }

    public int getClicks() {
        return clicks;
    }

    public int getOrders() {
        return orders;
    }

    public int getPayments() {
        return payments;
    }

    @Override
    public int compare(ClickOrderOfferSort that) {
        if (this.clicks - that.clicks != 0) {
            return this.clicks - that.clicks;
        } else if (this.orders - that.orders != 0) {
            return this.orders - that.orders;
        } else {
            return this.payments - that.payments;
        }
    }

    @Override
    public boolean $less(ClickOrderOfferSort that) {
        if (this.clicks < that.clicks) {
            return true;
        } else if (this.clicks == that.clicks && this.orders < that.orders) {
            return true;
        } else if (this.clicks == that.clicks && this.orders == that.orders && this.payments < that.payments) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(ClickOrderOfferSort that) {
        if (this.clicks > that.clicks) {
            return true;
        } else if (this.clicks == that.clicks && this.orders > that.orders) {
            return true;
        } else if (this.clicks == that.clicks && this.orders == that.orders && this.payments > that.payments) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(ClickOrderOfferSort that) {
        return this.$less(that) || (this.clicks == that.clicks && this.orders == that.orders && this.payments == that.payments);
    }

    @Override
    public boolean $greater$eq(ClickOrderOfferSort that) {
        return this.$greater(that) || (this.clicks == that.clicks && this.orders == that.orders && this.payments == that.payments);
    }

    @Override
    public int compareTo(ClickOrderOfferSort that) {
        if (this.clicks - that.clicks != 0) {
            return this.clicks - that.clicks;
        } else if (this.orders - that.orders != 0) {
            return this.orders - that.orders;
        } else {
            return this.payments - that.payments;
        }
    }

    @Override
    public String toString() {
        return "ClickOrderOfferSort{" +
                "clicks=" + clicks +
                ", orders=" + orders +
                ", payments=" + payments +
                '}';
    }
}

