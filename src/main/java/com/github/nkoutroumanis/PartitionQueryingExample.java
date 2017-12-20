/*
 * Copyright 2017 nicholaskoutroumanis.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.nkoutroumanis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 *
 * @author nicholaskoutroumanis
 */
public final class PartitionQueryingExample {

    private static final String triplatesAbsolutePath = "/Users/nicholaskoutroumanis/Desktop/aisEncodedDataSample/ais_jan2016_20170329_encoded.sample.txt";//absolute path of the txt containing triplates
    private static final int numberOfPartitions = 1;
    private static final String sqlResults = "/Users/nicholaskoutroumanis/Desktop/SQL Results";
    private static final String dictionaryPath = "/Users/nicholaskoutroumanis/Desktop/aisEncodedDataSample/dictionary.txt";

    public static void main(String args[]) throws IOException {    

        //Dictionary Construction
        Map<String,String> dictionary = new HashMap<>();
        Files.lines(Paths.get(dictionaryPath)).forEach(new Consumer<String>(){
            @Override
            public void accept(String s) {
                dictionary.put(s.substring(0, s.lastIndexOf("	")+1), s.substring(s.lastIndexOf("	")+1));
            }
        }
        );
        
        //Initialization of Apache Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hiveCtx = new HiveContext(sc.sc());
        //Read txt - per line and Separate every Line to a triplet of numbers
        JavaRDD<String[]> wordsPerLine = sc.textFile(triplatesAbsolutePath).map(new Function<String, String[]>() {
            @Override
            public String[] call(String line) {
                return line.split(" ");
            }

        });

        //Construct Pair RDD having as Key a Subject
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> pairs = wordsPerLine.mapToPair(
                new PairFunction<String[], Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Integer>> call(String[] x) throws Exception {
                return new Tuple2(Integer.parseInt(x[0]), new Tuple2(Integer.parseInt(x[1]), Integer.parseInt(x[2])));
            }
        }
        );

        JavaRDD<Row> positiveSubjects = pairs.filter(new Function<Tuple2<Integer, Tuple2<Integer, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Tuple2<Integer, Integer>> tuple) {
                return (tuple._1 >= 0);
            }
        }).sortByKey(true, numberOfPartitions).mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Row>() {
            @Override
            public Iterable<Row> call(Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> t) {
                List<Row> i = new ArrayList<Row>();

                Tuple2<Integer, Tuple2<Integer, Integer>> x;
                while (t.hasNext()) {
                    x = t.next();
                    i.add(RowFactory.create(x._1, x._2._1, x._2._2));
                }
                return i;
            }
        }, true);
        System.out.println("" + positiveSubjects.collect());

        JavaRDD<Row> negativeSubjects = pairs.filter(new Function<Tuple2<Integer, Tuple2<Integer, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Tuple2<Integer, Integer>> tuple) {
                return (tuple._1 < 0);
            }
        }).map(new Function<Tuple2<Integer, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<Integer, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }

        });
        System.out.println("Arnitika Count" + negativeSubjects.count());
        final Broadcast<JavaRDD<Row>> x = sc.broadcast(negativeSubjects);
        final Broadcast<Map<String,String>> y = sc.broadcast(dictionary);

        //contstruct the column names
        StructType customSchema = new StructType(new StructField[]{
            new StructField("Subject", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("Predicate", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("Object", DataTypes.IntegerType, true, Metadata.empty()),});

        DataFrame dfPositive = hiveCtx.createDataFrame(positiveSubjects, customSchema);
        hiveCtx.registerDataFrameAsTable(dfPositive, "Positive");

        DataFrame dfNegative = hiveCtx.createDataFrame(negativeSubjects, customSchema);
        hiveCtx.registerDataFrameAsTable(dfNegative, "Negative");

        hiveCtx.sql("SELECT Negative.Object FROM (SELECT Positive.Object FROM Negative "
                + " INNER JOIN Positive ON Negative.Object=Positive.Subject"
                + " WHERE Negative.Subject='-39' AND Negative.Predicate='-2' AND Positive.Predicate='-13'"
                + ") AS Table1"
                + " LEFT OUTER JOIN Negative ON(Negative.Subject=Table1.Object)"
                + "WHERE Negative.Predicate='-21'").toJavaRDD().saveAsTextFile(sqlResults); 
                

//        hiveCtx.sql("SELECT count(*) FROM (SELECT * FROM Negative "
//                + "WHERE (Negative.Predicate='-2' AND Negative.Subject='-39') OR Negative.Predicate='-21' "
//                + " UNION ALL SELECT * FROM Positive WHERE Positive.Predicate='-13'"
//                + ") AS Table2"
//                + " LEFT OUTER JOIN (SELECT * FROM Negative "
//                + "WHERE (Negative.Predicate='-2' AND Negative.Subject='-39') OR Negative.Predicate='-21' "
//                + " UNION ALL SELECT * FROM Positive WHERE Positive.Predicate='-13'"
//                + ") AS Table1"
//                + " ON Table2.Subject = Table1.Object"
//                + "").toJavaRDD().saveAsTextFile(sqlResults);    
    }
}
