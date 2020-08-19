package com.rakib;


import org.apache.spark.api.java.function.FilterFunction;
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

        //single expression by lamda
        Dataset<Row> rowDataset = dataSet.filter((FilterFunction<Row>) row -> row.getAs("Subject").equals("IT"));
        rowDataset.show();
        //multiple expression by lamda
        Dataset<Row> rowDataset1 = dataSet.filter((FilterFunction<Row>) row -> row.getAs("Subject").equals("IT")
                && Integer.parseInt(row.getAs("Score")) > 40);
        rowDataset1.show();


        session.close();
    }
}
