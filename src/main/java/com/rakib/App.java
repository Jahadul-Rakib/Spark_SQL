package com.rakib;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
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
        rowList.add(RowFactory.create(1, "Rakib7", 28, "IT"));
        rowList.add(RowFactory.create(2, "Rakib1", 23, "Eng"));
        rowList.add(RowFactory.create(3, "Rakib2", 25, "IT"));
        rowList.add(RowFactory.create(4, "Rakib3", 22, "EEE"));
        rowList.add(RowFactory.create(5, "Rakib4", 21, "EEE"));
        rowList.add(RowFactory.create(6, "Rakib5", 20, "CSE"));
        rowList.add(RowFactory.create(7, "Rakib6", 29, "Math"));

        //It will take Object Type of StructType
        StructField[] fields = new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("department", DataTypes.StringType, false, Metadata.empty())
        };

        StructType shema = new StructType(fields);
        Dataset<Row> dataset = session.createDataFrame(rowList, shema);
        dataset.createOrReplaceTempView("student_info");

        Dataset<Row> sql = session.sql("select department, COUNT(name) from student_info group by department");
        sql.show();

        session.close();
    }
}
