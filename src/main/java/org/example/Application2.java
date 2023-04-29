package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Application2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVille").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddVentes = sc.textFile("ventes.txt");
        JavaPairRDD<String,Double> rddVentesVille = rddVentes.mapToPair(line->{
            String[] ventes = line.toString().split(" ");
            String ville = ventes[1];
            String[] date = ventes[0].split("-");
            String year = date[2];
            double prix = Double.parseDouble(ventes[3]);
            return new Tuple2<>((ville+','+year),prix);
        });


        JavaPairRDD<String,Double> rddVentesVilleTotal = rddVentesVille.reduceByKey((aDouble, aDouble2) -> aDouble +aDouble2);
        JavaRDD<String> rddTotal = rddVentesVilleTotal.map(tuple-> tuple._1 +" "+ tuple._2.toString());

        rddTotal.saveAsTextFile("hdfs://localhost:9000/TotalVentesParVilleAnneeSPARK");
    }

}
