package com.example.dataloader.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * DataLoaderDAO
 */
public interface DataLoaderDAO {
    public Dataset<Row> load(String fileName);
}