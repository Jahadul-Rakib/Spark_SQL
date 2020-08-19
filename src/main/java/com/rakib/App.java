package com.rakib;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession session = SparkSession.builder().appName("SparkSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/")
                .getOrCreate();

        List<Row> rowList = new ArrayList<>();
        rowList.add(RowFactory.create(1, "Rakib7", 28, "IT", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(2, "Rakib1", 23, "Eng", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(3, "Rakib2", 25, "IT", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(4, "Rakib3", 22, "EEE", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(5, "Rakib4", 21, "EEE", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(6, "Rakib5", 20, "CSE", "2008-12-31 04:29:31"));
        rowList.add(RowFactory.create(7, "Rakib6", 29, "Math", "2008-12-31 04:29:31"));

        //It will take Object Type of StructType
        StructField[] fields = new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("department", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date", DataTypes.StringType, false, Metadata.empty())
        };

        StructType shema = new StructType(fields);
        Dataset<Row> dataset = session.createDataFrame(rowList, shema);
        dataset.createOrReplaceTempView("student_info");

        Dataset<Row> sql = session.sql("select department, COUNT(name) from student_info group by department");
        sql.show();

        Dataset<Row> sqlforGetDateTime = session.sql("select department,name, date_format(date, 'y') from student_info");
        sqlforGetDateTime.show();
        Dataset<Row> sqlforGetDateTime1 = session.sql("select department,name, date_format(date, 'yyyy') from student_info");
        sqlforGetDateTime1.show();
        Dataset<Row> sqlforGetDateTime2 = session.sql("select department,name, date_format(date, 'M') from student_info");
        sqlforGetDateTime2.show();
        Dataset<Row> sqlforGetDateTime22 = session.sql("select department,name, date_format(date, 'MMMM') from student_info");
        sqlforGetDateTime22.show();
        Dataset<Row> sqlforGetDateTime3 = session.sql("select department,name, date_format(date, 'dd') as date from student_info");
        sqlforGetDateTime3.show();

        //dataframe
        Dataset<Row> rowDataset = dataset.select("name", "age");
        rowDataset.show(3);
        //apply functionality need to use select expression
        Dataset<Row> rowDataset1 = dataset.selectExpr("name", "date_format(date, 'y') as Year");
        rowDataset1.show(3);
        //another approache
        Dataset<Row> rowDataset2 = dataset.select(
                functions.col("id"),
                functions.col("name"),
                functions.col("age"),
                functions.col("department"),
                functions.date_format(functions.col("date"), "MM").alias("Month"));
        rowDataset2 = dataset.groupBy(functions.col("department")).count();
        rowDataset2 = dataset.orderBy(functions.col("name"));
        rowDataset2.show();

        //pivot table
        Dataset<Row> dataset1 = dataset.groupBy("department").pivot("age").count();
        dataset1.show();

        //other pivot table way
        List<Object> list = new ArrayList<>();
        list.add("Rakib1");
        list.add("Rakib2");

        Dataset<Row> dataset2 = dataset.groupBy("department").pivot("name", list).count().na().fill(0);
        dataset2.show();



        session.close();
    }
}
