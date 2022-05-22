package com.example.utils;

import static com.example.utils.Constants.COLUMN_PARSE_MAX_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_MIN_UDF_NAME;
import static com.example.utils.Constants.COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UDFUtils {
    private SQLContext sqlContext;

    public UDFUtils(SQLContext _sqlContext) {
        this.sqlContext = _sqlContext;
    }

    public void registerColumnProcessYearsExpUdf() {
        this.sqlContext.udf().register(COLUMN_PARSE_YEARS_OF_EXP_UDF_NAME, (UDF1<String, String>) (yearsExp) -> {
            String years = yearsExp.toString();
            // Get 1-3 | 6-12 | null | 8+ | 14+ | 9 ->
            // means remove Yrs of Exp string w 5alas
            years = years.split(" ", 2)[0];
            if (years.equals("null"))
                return null;
            else if (years.indexOf('-') != -1) {
                String[] minmax = years.split("-");
                return minmax[0] + " " + minmax[1];
            } else if (years.indexOf('+') != -1) {
                years = years.substring(0, years.length() - 1);
                return years + " " + (Integer.parseInt(years) * 2);
                // return years + " " + years;
            } else {
                return years + " " + years;
            }
        }, DataTypes.StringType);

    }

    public void registerColumnMaxExpUdf() {

        this.sqlContext.udf().register(COLUMN_PARSE_MAX_UDF_NAME, (UDF1<String, Integer>) (yearsExp) -> {
            if (yearsExp == null)
                return null;
            else
                return Integer.parseInt(yearsExp.split(" ")[1]);
        }, DataTypes.IntegerType);
    }

    public void registerColumnMinExpUdf() {

        this.sqlContext.udf().register(COLUMN_PARSE_MIN_UDF_NAME, (UDF1<String, Integer>) (yearsExp) -> {
            if (yearsExp == null)
                return null;
            else
                return Integer.parseInt(yearsExp.split(" ")[0]);
        }, DataTypes.IntegerType);
    }
}
