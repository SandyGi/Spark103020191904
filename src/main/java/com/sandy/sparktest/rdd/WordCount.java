package com.sandy.sparktest.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = javaSparkContext.textFile("in/word_count.text");
        JavaRDD<String> word = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        Map<String, Long> wordCounts = word.countByValue();
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println("Entry : " + entry.getKey() + " ==> " + entry.getValue());
        }
    }
}
