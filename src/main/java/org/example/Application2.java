package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.w3c.dom.stylesheets.LinkStyle;
import scala.Tuple2;

import java.util.List;

public class Application2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVille");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rddVentes = sc.textFile("hdfs://localhost:9000/ventes.txt");
        JavaPairRDD<String,Double> rddVentesVille = rddVentes.mapToPair(line->{
            String[] ventes = line.toString().split(" ");
            String ville = ventes[1];
            String[] date = ventes[0].split("-");
            String year = date[2];
            double prix = Double.parseDouble(ventes[3]);
            return new Tuple2<>((ville+','+year),prix);
        });

        System.out.println("cas1"+rddVentes);
        System.out.println("cas2"+rddVentesVille);



        JavaPairRDD<String,Double> rddVentesVilleTotal = rddVentesVille.reduceByKey((aDouble, aDouble2) -> aDouble +aDouble2);

        List<String> rddTotal = rddVentesVilleTotal.map(tuple-> tuple._1 +" "+ tuple._2.toString()).collect();

        //rddTotal.saveAsTextFile("TotalVentesParVilleAnnee");

        for ( String rs : rddTotal) {
            System.out.println(rs);
        }



    }

}
