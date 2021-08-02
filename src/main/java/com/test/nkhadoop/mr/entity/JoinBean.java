package com.test.nkhadoop.mr.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// 容纳两个二维表中的所有的字段的容器类
public class JoinBean implements Writable,Comparable {
    private String orderID;
    private String productID;
    private String category;
    private String amount;
    private String flag;    // 指明填充字段的数据的来源

    public JoinBean() {
    }

    public JoinBean(String orderID, String productID, String category, String amount, String flag) {
        this.orderID = orderID;
        this.productID = productID;
        this.category = category;
        this.amount = amount;
        this.flag = flag;
    }

    public String getOrderID() {
        return orderID;
    }

    public void setOrderID(String orderID) {
        this.orderID = orderID;
    }

    public String getProductID() {
        return productID;
    }

    public void setProductID(String productID) {
        this.productID = productID;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "JoinBean{" +
                "orderID='" + orderID + '\'' +
                ", productID='" + productID + '\'' +
                ", category='" + category + '\'' +
                ", amount='" + amount + '\'' +
                ", flag='" + flag + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 逐个写入属性
        dataOutput.writeUTF(orderID);
        dataOutput.writeUTF(productID);
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 按照写入顺序逐一读出
        orderID=dataInput.readUTF();
        productID=dataInput.readUTF();
        category=dataInput.readUTF();
        amount=dataInput.readUTF();
        flag=dataInput.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
