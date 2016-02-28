package com.mansourahmed.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.time.LocalTime;
import java.util.Arrays;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

/**
 * Created by Mansour Ahmed on 27/02/16.
 */
public class SparkSandbox {

    private static final String LOCAL_FILE = "/home/mansour/dev/Hadoop-Spark/src/main/resources/data.parquet";
    private static final String HADOOP_FILE = "hdfs://127.0.0.1:54310/data.parquet";

    public static void main(String[] args) throws IOException {
        SQLContext sqlContext = initialise();
        DataFrame dataFrame = sqlContext.parquetFile(HADOOP_FILE);
        getRowCount(dataFrame);
        getColumns(dataFrame);
        getSample(dataFrame);
        getSomeAggregates(dataFrame);
        dataFrame.registerTempTable("ticks");
        printFirst20Rows(sqlContext);
        System.in.read();
    }

    private static void printFirst20Rows(SQLContext sqlContext) {
        sqlContext.sql("select * from ticks").show();
    }

    private static void getSomeAggregates(DataFrame dataFrame) {
        long startTime = System.currentTimeMillis();
        dataFrame.select(avg("bid")).show();
        dataFrame.select(max("bid")).show();
        dataFrame.select(min("bid")).show();

        dataFrame.select(avg("ask")).show();
        dataFrame.select(max("ask")).show();
        dataFrame.select(min("ask")).show();
        printTimeTaken(startTime);
    }

    private static void getColumns(DataFrame dataFrame) {
        long startTime = System.currentTimeMillis();
        String[] columns = dataFrame.columns();
        System.out.println(Arrays.asList(columns));
        printTimeTaken(startTime);
    }

    private static void getSample(DataFrame dataFrame) {
        long startTime = System.currentTimeMillis();
        System.out.println(dataFrame.first());
        printTimeTaken(startTime);
    }

    private static void getRowCount(DataFrame dataFrame) {
        long startTime = System.currentTimeMillis();
        System.out.println("Rows:" + dataFrame.count());
        printTimeTaken(startTime);
    }

    private static void printTimeTaken(long startTime) {
        long endTime = System.currentTimeMillis();
        long millisTaken = endTime - startTime;
        System.out.println("Time taken: " + (millisTaken/1000) + " seconds");
    }

    private static SQLContext initialise() {
        SparkConf conf = new SparkConf().setAppName("com.mansourahmed.bigdata").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        return new SQLContext(jsc);
    }

}