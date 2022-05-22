package com.example.dataloader;

import com.example.dataloader.dao.DataLoaderDAO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * WuzzufJobsCsv
 */
public class WuzzufJobsCsv implements DataLoaderDAO {

    public WuzzufJobsCsv() {
        super();
    }

    public Dataset<Row> load(String filename) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .csv(filename);

        return dataset;
    }
}