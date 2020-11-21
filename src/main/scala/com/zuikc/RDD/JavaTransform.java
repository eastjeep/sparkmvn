package com.zuikc.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class JavaTransform {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] inp = {1, 2, 3, 4, 5, 6};
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(inp));
        System.out.println(rdd.count());
        System.out.println(rdd.map(x->x+1).reduce((a, b) -> a + b));

        sc.stop();
    }
}
