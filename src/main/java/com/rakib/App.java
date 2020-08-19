package com.rakib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.spark.sql.functions.*;
public class App {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession session = SparkSession.builder().appName("SparkSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/")
                .getOrCreate();

        //*******************complex agreegations*****************
        Dataset<Row> dataset = session.read().option("header", true).option("inferSchema", true).csv("src/main/resources/student.csv");
        Dataset<Row> agg = dataset.groupBy(functions.col("Subject")).agg(
                functions.max("Score").cast(DataTypes.IntegerType).alias("Max_Score"),
                functions.min("Score").cast(DataTypes.IntegerType).alias("Min_Score")
        );
        //add column using Lit
        Dataset<Row> withColumn = agg.withColumn("pass", functions.lit("yes"));
        withColumn.show();

        //*************User Define Function**********************
        session.udf().register("examResult", (Integer score)-> score >= 70, DataTypes.BooleanType);

/*        UserDefinedFunction examResult = udf((int score)->
            score>= 70 return true , DataTypes.BooleanType);*/

        Dataset<Row> rowDataset = dataset.withColumn("pass", callUDF("examResult", col("Score")));
        rowDataset.show();



        session.close();
    }
}
