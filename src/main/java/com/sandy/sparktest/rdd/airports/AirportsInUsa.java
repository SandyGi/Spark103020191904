package com.sandy.sparktest.rdd.airports;

import com.sandy.sparktest.utils.Utils;
import org.apache.ivy.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsa {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("airportsWithCityName").setMaster("local[2]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> airports = javaSparkContext.textFile("in/airports.text");
        JavaRDD<String> airportsInUsa = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
//        System.out.println(airportsInUsa.toString());
        JavaRDD<String> airportsWithCityName = airportsInUsa.map(line -> line.split(Utils.COMMA_DELIMITER)[1]);
//            System.out.println(split[1]);
//            return StringUtils.join(new String[]{split[1], split[2]}, ",");
//        );
        airportsWithCityName.saveAsTextFile("out/airportsWithCityName");

    }
}
