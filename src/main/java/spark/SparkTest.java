package spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkTest {
    public static void main(String[] args) {
        String logFile = "/usr/lib/spark/spark-2.3.1-bin-hadoop2.7/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .config("spark.master","local")
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();
        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();
        System.out.println(logData.count());
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }
}
