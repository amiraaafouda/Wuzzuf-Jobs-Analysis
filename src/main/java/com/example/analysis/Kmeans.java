package com.example.analysis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Kmeans {
    public static String calculateKMeans(Dataset<Row> dataset) {
        // Loads data.
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        // sparkSession.sparkContext();
        Dataset<Row> convert = sparkSession.sql("select Title, Company from WUZZUF_DATA");
        StringIndexerModel indexer = new StringIndexer()
                .setInputCols(new String[] { "Title", "Company"})
                .setOutputCols(new String[] { "TitleConvert", "CompanyConvert"})
                .fit(convert);

        Dataset<Row> indexed = indexer.transform(convert);
        // indexed.show();
        // indexed.printSchema();
        Dataset<Row> factorized = indexed.select("TitleConvert", "CompanyConvert");
        // factorized.show();

        // System.out.println("+++++========++++++++kmeans");

        JavaRDD<Vector> parsedData = factorized.javaRDD().map((Function<Row, Vector>) s -> {
            double[] values = new double[2];

            values[0] = Double.parseDouble(s.get(0).toString());
            values[1] = Double.parseDouble(s.get(1).toString());

            return Vectors.dense(values);});

        parsedData.cache();
        // Cluster the data into four classes using KMeans
        int numClusters = 4;
        int numIterations = 30;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WithinError = clusters.computeCost(parsedData.rdd());
        // System.out.println("Within Set Sum of Squared Errors = " + WithinError);

        String output = String.format("<h1 style=\"text-align:center;font-family:verdana;background-color:LightPink;\">%s</h1>","K-means to predict jobs class and company class") +
                  "<table style=\"border:1px solid black;width:100%;text-align: center\">";

        output += AnalysisHelper.getInstance().datasetToTable(indexed.limit(20));
        output += "<html><body>";
        output += "<h1 style=\"text-align:center;background-color:LightPink;\">"+"Cluster centers"+"</h1><br><p>";
        // System.out.println("Cluster centers and Error:");
        int i=1;
        for (Vector str : clusters.clusterCenters()){
            // System.out.println(" " + str);
            output+= String.format("<h2 style=\"text-align:center;\">Cluster %d = %s</h2>", i,str.toString().replace("[","(").replace("]",")")) ;
            //output += str.toString();
            //output += "<br>";
            i+=1;
        }
        output+=String.format("<h2 style=\"text-align:center;\"> Within Set Sum of Square Error = %f</h2>",WithinError)+
                "<table style=\"border:1px solid black;width:100%;text-align: center\">";

        //output += "<br>" + "Within Set Sum of Square Error: " + WithinError+"<br>";
       // output += "</p></body></html>";

        return output;

    }
}
