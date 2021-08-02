package com.test.nkhadoop.mr.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// ����������ά���е����е��ֶε�������
public class JoinBean implements Writable,Comparable {
    private String orderID;
    private String productID;
    private String category;
    private String amount;
    private String flag;    // ָ������ֶε����ݵ���Դ

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
        // ���д������
        dataOutput.writeUTF(orderID);
        dataOutput.writeUTF(productID);
        dataOutput.writeUTF(category);
        dataOutput.writeUTF(amount);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // ����д��˳����һ����
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
