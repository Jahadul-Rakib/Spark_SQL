package com.rakib;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

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

        //Most Uses filter approach
        Column columnSubject = dataSet.col("Subject");
        Column columnScore = dataSet.col("Score");

        Dataset<Row> rowDataset = dataSet.filter(columnSubject.startsWith("I")
                                                 .and(columnScore.$less$eq(40))
                                                 .or(columnScore.geq(60)));
        rowDataset.show();

        //Another Way using sql.function
        Column subject = functions.col("Subject");
        Column score = functions.col("Score");
        Dataset<Row> rowDataset1 = dataSet.filter(subject.startsWith("H")
                                          .and(score.geq(70)));
        rowDataset1.show();

        //Another Way using static import
        Column subjectS = col("Subject");
        Column scoreS = col("Score");
        Dataset<Row> rowDataset3 = dataSet.filter(subjectS.startsWith("H")
                                          .and(scoreS.$greater(70)));
        rowDataset3.show();


        session.close();
    }
}
