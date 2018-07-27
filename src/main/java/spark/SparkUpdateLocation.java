package spark;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;

public class SparkUpdateLocation {
    static HashMap<String,String> hm = new HashMap<>();

    public static void main(String[] args) {
//        String filePath = "/home/hung/Documents/iplocationVN/parquet_logfile_at_09h_00.snap";
//        repairDataFromParquetFileLocal(filePath);
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("IP Log")
//                .config("spark.sql.parquet.binaryAsString", "true")//.config("spark.master","local")
//                .config("spark.yarn.access.hadoopFileSystems", "hdfs://192.168.23.200:9000")
//                .getOrCreate();
        System.out.println("--------- Demo! ---------");
    }

//  Loc du lieu tu mot file parquet tren local roi ghi du lieu ra file cvs gom co ip, city, region
    private static  void repairDataFromParquetFileLocal(String filePath){
        SparkSession spark = SparkSession
                .builder()
                .appName("IP Log")
                .config("spark.sql.parquet.binaryAsString", "true").config("spark.master","local")
                .getOrCreate();
        Dataset<Row> logDF = spark.read().parquet(filePath);
        Set<String> ipList = new HashSet<>();
        Dataset<Row> filteredDS = logDF.distinct().filter((FilterFunction<Row>) row -> checkIpRequirement(row.getString(0), row.getString(2)));
        JavaRDD<String> ipDS = filteredDS.select("ip").as(Encoders.STRING()).javaRDD();

        ArrayList<String> list = new ArrayList<>();

        JavaPairRDD<String, Integer> counts = ipDS
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);
        for (Tuple2<String, Integer> s : counts.collect()) {
            if (s._2 > 2) ipList.add(s._1);
            if (s._2==1) list.add(s._1);
        }
        Dataset<Row> finalSet = filteredDS.filter((FilterFunction<Row>) row -> !checkDuplicate(row.getString(0), ipList)).sort("ip");
//        finalSet.show(1000, false);
        finalSet.createOrReplaceTempView("l1");

        Dataset<Row> list1 = spark.sql("select ip,count(ip) as count from l1 group by ip");
//        list1.show();
        Dataset<Row> data1 = list1.join(finalSet,finalSet.col("ip").equalTo(list1.col("ip"))).where("count=1");
        Dataset<Row> data2 = list1.join(finalSet,finalSet.col("ip").equalTo(list1.col("ip"))).where("count=2");
        data1.show(1000,false);
        data2.show(1000,false);

        //loc data2
        data2.foreach(row -> {
            String ip = row.getString(0);
            String location = row.getString(4);
            String[] s = location.split(",");
            String key;
            if (s.length>2)
                key=ip+":"+s[1];
            else key = ip+":"+s[0];
            if (hm.containsKey(key) && hm.get(key)!=s[0]) hm.put(key,"-");
            else hm.put(key,s[0]);
        });
        data1.foreach(row -> {
            String ip = row.getString(0);
            String location = row.getString(4);
            String[] s = location.split(",");
            String key;
            if (s.length>2)
                key=ip+":"+s[1];
            else key = ip+":"+s[0];
            hm.put(key,s[0]);
        });
        System.out.println(hm.size());
        cvsWriter(hm);
        spark.stop();
    }
    //ghi vao file cvs
    private static void cvsWriter(HashMap<String,String> hm) {
        try {
            //We have to create the CSVPrinter class object
            LocalDate today = LocalDate.now();
            Writer writer = Files.newBufferedWriter(Paths.get("/home/hung/Documents/ipgooglenew/ipgoogle_"+today.getDayOfMonth()+"_"+today.getMonth()+"_"+today.getYear()+".csv"));
            CSVPrinter csvPrinter = new CSVPrinter(writer,
                    CSVFormat.DEFAULT.withHeader("ip", "city_name", "region_name"));

            //Writing IP in the generated CSV file
            hm.forEach((key,value)->{
                if (value!="-") {
                    String[] ippro = key.split(":");
                    try {
                        csvPrinter.printRecord(ippro[0], value, ippro[1]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            csvPrinter.flush();
            System.out.println("Write csv file by using new Apache lib successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /* kiem tra ip thuoc private ip va city ko hop le */

    private static boolean checkIpRequirement(String ip, String city) {
        if (city.split(",").length == 1) return false;
        if (ip.isEmpty()) return false;
        long i = ipToLong(ip);
        boolean req1 = i < 167772160 || i > 184549375;
        boolean req2 = i < 2886729728L || i > 2887778303L;
        boolean req3 = i < 3232235520L || i > 3232301055L;
        return req1 && req2 && req3;
    }

    /* chuyen ip sang dang Long */

    private static long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return result;
    }
    private static boolean checkDuplicate(String ip, Set<String> ipList) {
        return ipList.contains(ip);
    }
}
