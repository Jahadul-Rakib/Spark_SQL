package com.rakib;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession session = SparkSession.builder().appName("SparkSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/")
                .getOrCreate();

        Dataset<Row> dataSet = session.read().option("header", true).csv("src/main/resources/student.csv");
        dataSet.createOrReplaceGlobalTempView("Student_Table");
        dataSet.show();

        long numberOfRows = dataSet.count();
        System.out.println("Total : " + numberOfRows);

        session.close();
    }
}
