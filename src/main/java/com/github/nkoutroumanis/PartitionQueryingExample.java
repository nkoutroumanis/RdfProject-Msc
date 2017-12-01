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

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import static org.apache.spark.api.java.StorageLevels.MEMORY_ONLY;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author nicholaskoutroumanis
 */
public final class PartitionQueryingExample {
    private static final String triplatesAbsolutePath = "/Users/charalambostilliros/Downloads/aisEncodedDataSample/ais_jan2016_20170329_encoded.sample.txt";//absolute path of the txt containing triplates
    private static final int numberOfPartitions = 2;
     public static void main(String args[]) {
        //Initialization of Apache Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read txt - per line and Separate every Line to a triplet of numbers
        JavaRDD<List<String>> wordsPerLine = sc.textFile(triplatesAbsolutePath).map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String line) {
                return Arrays.asList(line.split(" "));
            }

        });
        
        //Construct Pair RDD having as Key a Subject
        JavaPairRDD<Integer, List<Integer>> pairs = wordsPerLine.mapToPair(
                new PairFunction<List<String>, Integer, List<Integer>>() {
            @Override
            public Tuple2<Integer, List<Integer>> call(List<String> x) throws Exception {

                return new Tuple2(Integer.getInteger(x.get(0)), Arrays.asList(Integer.getInteger(x.get(1)),Integer.getInteger(x.get(2))));
            }
        }
        );
        
        JavaPairRDD<Integer, List<Integer>> positiveSubjects = pairs.filter(new Function<Tuple2<Integer, List<Integer>>,Boolean>(){
                @Override
                public Boolean call(Tuple2<Integer, List<Integer>> tuple) {
                    return (tuple._1>=0);
                }            
        }).sortByKey(true, numberOfPartitions).persist(MEMORY_ONLY);
        
        JavaPairRDD<Integer, List<Integer>> negativeSubjects = pairs.filter(new Function<Tuple2<Integer, List<Integer>>,Boolean>(){
                @Override
                public Boolean call(Tuple2<Integer, List<Integer>> tuple) {
                    return (tuple._1<0);
                }            
        });
        
     }
}
