package com.test.nkhadoop.mr.utils;

import java.net.URL;

public class MyPath {
    public static String getRootPath(){
        URL url = MyPath.class.getResource("/");
        String myPath = url.getPath();
        myPath = myPath.substring(1, myPath.lastIndexOf("classes"));
        return myPath;
    }

    public static void main(String[] args) {
        System.out.println(MyPath.getRootPath());
    }
}