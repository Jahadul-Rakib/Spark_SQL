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

        Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/student.csv");
        //Its send the data to session and then perform sql into session
        dataset.createOrReplaceTempView("my_students");

        Dataset<Row> rowDataset = session.sql("select * from my_students order by Subject");
        rowDataset.show();
        Dataset<Row> rowDataset1 = session.sql("select avg(Score) from my_students");
        rowDataset1.show();


        session.close();
    }
}
