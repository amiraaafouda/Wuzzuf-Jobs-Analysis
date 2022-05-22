package com.example.analysis;

import com.example.dataloader.WuzzufJobsCsv;
import com.example.dataloader.dao.DataLoaderDAO;
import com.example.utils.UDFUtils;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import static java.util.stream.Collectors.*;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
// import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.example.utils.Constants.COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_MAX_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_MIN_UDF_NAME;
import static com.example.utils.Constants.POPULAR_TITLES_CHART;
import static com.example.utils.Constants.POPULAR_SKILLS_CHART;
import static com.example.utils.Constants.POPULAR_AREAS_CHART;
import static com.example.utils.Constants.IMG_PATH;;

public class WuzzufJobsAnalysis {
    Dataset<Row> wuzzufData;
    SparkSession spark;
    UDFUtils udfUtil;
    final String path = "src/main/resources/Wuzzuf_Jobs.csv";

    private static WuzzufJobsAnalysis instance;

    public static WuzzufJobsAnalysis getInstance() {
        if (instance == null) {
            instance = new WuzzufJobsAnalysis();
        }
        return instance;
    }

    private WuzzufJobsAnalysis() {
        spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        udfUtil = new UDFUtils(spark.sqlContext());
        udfUtil.registerColumnProcessYearsExpUdf();
        udfUtil.registerColumnMaxExpUdf();
        udfUtil.registerColumnMinExpUdf();
    }

    public Dataset<Row> readData() {
        DataLoaderDAO loader = new WuzzufJobsCsv();
        wuzzufData = loader.load(path);
        return wuzzufData;
    }


    public List<Dataset<Row>> cleanData(Dataset<Row> df) {

        // Original data
        List<Dataset<Row>> dfs = new ArrayList<>();
        dfs.add(df);

        // Remove Duplicates:
        df = df.dropDuplicates();
        dfs.add(df);

        //Remove Null Data
        String[] strArray = new String[] {"MaxYearsExp", "MinYearsExp"};
        df = df.na().drop(strArray);
        dfs.add(df);

        return dfs;
    }

    public void createTempView(Dataset<Row> df, String name){
        df.createOrReplaceTempView(name);
    }

    public Dataset<Row> encodeCategories(Dataset<Row> df) {
        StringIndexer typeIndexer = new StringIndexer().setInputCol("Type").setOutputCol("type-factorized")
                .setHandleInvalid("keep");
        df = typeIndexer.fit(df).transform(df);
        StringIndexer levelIndexer = new StringIndexer().setInputCol("Level").setOutputCol("level-factorized")
                .setHandleInvalid("keep");
        df = levelIndexer.fit(df).transform(df);
        df = df.drop("Type", "Level");

        return df;
    }

    public Dataset<Row> factoriesYearsOfExp(Dataset<Row> df) {
        df = df.withColumn("MinMaxYearsExp",
                callUDF(COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME, col("YearsExp")));
        df = df.withColumn("MaxYearsExp",
                callUDF(COLUMN_PARSE_MAX_UDF_NAME, col("MinMaxYearsExp")));
        df = df.withColumn("MinYearsExp",
                callUDF(COLUMN_PARSE_MIN_UDF_NAME, col("MinMaxYearsExp")));
        df = df.drop("MinMaxYearsExp", "YearsExp");

        return df;
    }


    // return some of first rows from data ---- it need number of line that you want to display.
    public Dataset<Row> getHeadData(int number){
        // Create view and execute query to display first n rows of WuzzufJobs data:
        this.wuzzufData.createOrReplaceTempView ("WuzzufJobs");
        return this.spark.sql("SELECT * FROM WuzzufJobs LIMIT "+ number+";");
    }

    public Dataset<Row> getTypedDataset(String name) {
        Dataset<Row> typedDataset = spark.sql("SELECT "
                + "cast (Title as string) Title, "
                + "cast (Company as string) Company, "
                + "cast (Location as string) Location, "
                + "cast (Type as string) Type, "
                + "cast (Level as string) Level, "
                + "cast (YearsExp as string) YearsExp, "
                + "cast (Country as string) Country, "
                + "cast (Skills as string) Skills FROM " + name);
        return typedDataset;
    }



//    private String getOutput(Dataset<Row> ds, String header){
//        // Output
//        List<String> data = ds.toJSON().collectAsList();
//
//        String output = "<html><body>";
//        output += "<h1>"+header+"</h1><br><p>";
//        for (String str : data){
//            String s = str.toString().replace("\"", "").replace(":", "-->");
//            String[] strList = s.split(",",8);
//            for (String attrib : strList){
//                output = output + attrib;
//                output += "<br>";
//            }
//            output += "<br>";
//        }
//        output += "</p></body></html>";
//        return output;
//    }


    public Dataset<Row> jobsByCompany(Dataset<Row> df){
        Dataset<Row> company = df.groupBy("Company").count().orderBy(col("count").desc());
        // company.printSchema();
        // PieCharts.popularCompanies(company);
        return company;
    }

    public Dataset<Row> FindMostPopular(Dataset<Row> df, String ColName) {
        return df.groupBy(ColName).count().sort(col("count").desc());
    }

    public Dataset<Row> MostPopularTitles(Dataset<Row> df) {
        return FindMostPopular(df, "Title");
    }

    public Dataset<Row> MostPopularAreas(Dataset<Row> df) {
        return FindMostPopular(df, "Location");
    }

    public void DrawBarChart(Object df, String Xcol, String Ycol, String titleName, String xAxisTitle, String yAxisTitle, String SeriesName) {
        String imgPath = IMG_PATH + titleName;
        List<String> Col_Selection = new ArrayList<String>();
        List<Long> counts = new ArrayList<Long>();
        Boolean skils = false;
        if (df.getClass().getName().equals("org.apache.spark.sql.Dataset")) {

            Dataset<Row> Popular_df = ((Dataset<Row>) df).limit(5);
            Col_Selection = Popular_df.select(Xcol).as(Encoders.STRING()).collectAsList();
            counts = Popular_df.select(Ycol).as(Encoders.LONG()).collectAsList();
        
        } else if (df.getClass().getName().equals("java.util.LinkedHashMap")) {
        
            skils = true;
            int i = 0;
            for (Entry<String, Integer> entry : ((Map<String, Integer>) df).entrySet()) {
                if (i >= 20)
                    break;
                Col_Selection.add(entry.getKey());
                counts.add(entry.getValue().longValue());
                i++;
            }
        
        }

        CategoryChart chart = new CategoryChartBuilder()
                .width(900)
                .height(600)
                .title(titleName.replace("-", ""))
                .xAxisTitle(xAxisTitle)
                .yAxisTitle(yAxisTitle).build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideN);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        if (skils)
            chart.getStyler().setXAxisLabelRotation(90);
        chart.addSeries(SeriesName, Col_Selection, counts);
        try {
            BitmapEncoder.saveBitmap(chart, imgPath, BitmapEncoder.BitmapFormat.JPG);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // new SwingWrapper(chart).displayChart();
    }

    public void JobTitlesBarGraph(Dataset<Row> df) {
        Dataset<Row> MostTitles_df = MostPopularTitles(df);
        DrawBarChart(MostTitles_df, "Title", "count", POPULAR_TITLES_CHART, "Titles", "Count", "Titles's Count");

    }

    public void JobSkillsBarGraph(Dataset<Row> df) {
        Map<String, Integer> map = this.mostPopularSkills(df);
        DrawBarChart(map, "", "", POPULAR_SKILLS_CHART, "Skills", "Count", "Skill's Count");
    }

    public void AreasCountBarGraph(Dataset<Row> df) {
        Dataset<Row> MostAreas_df = MostPopularAreas(df);
        DrawBarChart(MostAreas_df, "Location", "count", POPULAR_AREAS_CHART, "Areas", "Count", "Area's Count");

    }

    public Map<String, Integer> mostPopularSkills(Dataset<Row> df) {

        List<Row> skillSet = df.select("Skills").collectAsList();
        List<String> allSkils = new ArrayList<String>();
        String skill;
        for (Row row : skillSet) {
            skill = row.get(0).toString();
            String[] subs = skill.split(",");
            for (String word : subs) {
                allSkils.add(word);
            }
        }
        Map<String, Integer> mapAllSkills = allSkils.stream()
                .collect(groupingBy(Function.identity(), summingInt(e -> 1)));
        // Sort the map descending
        Map<String, Integer> sorted_skillset = mapAllSkills
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(100)
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
        // int idx = 0;
        // System.out.println("=============== Most Repaeated Skills ==============");
        // for (Map.Entry<String, Integer> entry : sorted_skillset.entrySet()) {
        //     System.out.println(entry.getKey() + " : " + entry.getValue());
        //     if (idx == 30) {
        //         break;
        //     }
        //     idx++;
        // }
        return (sorted_skillset);
    }

    public String getKMeans(Dataset<Row> df){
        createTempView(df, "WUZZUF_DATA");
        String kmeans = (new Kmeans()).calculateKMeans(df);
        // System.out.println(kmeans);
        return kmeans;
    }


}
