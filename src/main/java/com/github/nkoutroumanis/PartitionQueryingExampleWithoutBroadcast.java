package com.github.nkoutroumanis;

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
import com.github.nkoutroumanis.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
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
public final class PartitionQueryingExampleWithoutBroadcast {

    private static final String triplatesAbsolutePath = "/Users/nicholaskoutroumanis/Desktop/aisEncodedDataSample/ais_jan2016_20170329_encoded.sample.txt";//absolute path of the txt containing triplates
    private static final int numberOfPartitions = 5;
    private static final String sqlResults = "/Users/nicholaskoutroumanis/Desktop/SQL Results";
    public static final String dictionaryPath = "/Users/nicholaskoutroumanis/Desktop/aisEncodedDataSample/dictionary.txt";

    public static void main(String args[]) throws IOException {

        //Dictionary Construction
        Map<Integer, String> dictionary = new HashMap<>();
        Files.lines(Paths.get(dictionaryPath)).forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                dictionary.put(Integer.parseInt(s.split("	", 2)[0]), s.split("	", 2)[1]);
            }
        }
        );

        //Delete SQL Results Folder if Exist
        FileUtils.deleteDirectory(new File(sqlResults));

        //Initialization of Apache Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark");

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

        JavaRDD<Row> subjects = pairs.sortByKey(true, numberOfPartitions).mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>, Row>() {
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
        //subjects.saveAsTextFile(sqlResults);

        System.out.println("PARTITIONS: " + subjects.rdd().getPartitions().length);

        final Broadcast<Map<Integer, String>> y = sc.broadcast(dictionary);

        //Contstruct the Column Names
        StructType customSchema = new StructType(new StructField[]{
            new StructField("Subject", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("Predicate", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("Object", DataTypes.IntegerType, true, Metadata.empty()),});

        DataFrame dfsubjects = hiveCtx.createDataFrame(subjects, customSchema);
        hiveCtx.registerDataFrameAsTable(dfsubjects, "table");

        long startTime = System.currentTimeMillis();

        DataFrame results = hiveCtx.sql("SELECT * FROM table INNER JOIN table t1 ON table.object=t1.subject INNER JOIN table t2 ON t1.object=t2.subject WHERE table.subject='-39' AND table.predicate='-2' AND t1.predicate='-13' AND t2.predicate='-21'");

        System.out.println("EXECUTION TIME: " + (System.currentTimeMillis() - startTime));

        //Procedure Of Decoding
//        JavaRDD<Row> s = results.toJavaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, Row>() {
//            @Override
//            public Iterable<Row> call(Iterator<Row> t) throws Exception {
//                Collection<Row> rows = new ArrayList<Row>();
//                while (t.hasNext()) {
//                    //for every row get all the elements it has and decode them
//                    Collection<String> elementsOfARow = new ArrayList<String>();
//                    Row row = t.next();
//                    for (int i = 0; i < row.size(); i++) {
//                        elementsOfARow.add(y.getValue().get(row.getInt(i)));
//                    }
//                    rows.add(RowFactory.create(elementsOfARow));
//                }
//                return rows;
//            }
//        }, true);
//
//        
//        s.saveAsTextFile(sqlResults);
    }
}
