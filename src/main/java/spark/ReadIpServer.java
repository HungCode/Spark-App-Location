package spark;

import model.IpRange;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;

public class ReadIpServer {

    static HashMap<String,String> hm = new HashMap<>();

    private static List<Long> ipLeft;
    private static List<IpRange> rangeList;
    private static Map<Long, Integer> ipMap;


    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: App <location file> <pageviewlog> <true | false : write to file > <show n rows dataset>");
            System.exit(1);
        }

        SparkSession session = SparkSession
                .builder()
                .appName("Spark Parquet")
                .config("spark.yarn.access.hadoopFileSystems", "hdfs://192.168.23.200:9000")
                .config("spark.master", "local")
                .getOrCreate();

        /*
         * source example : hdfs://192.168.23.200:9000/data/Parquet/PageViewV1/2018_07_16
         * http://hadoop23200:50070/explorer.html#/data/Parquet/DSPLog/2018_07_26
         */

        ipLeft = new ArrayList<>();
        Dataset<String> rangeDS = session.read().textFile(args[0]);
        List<String> rangeRawList = rangeDS.collectAsList();
        rangeList = rawIpRangeConvert(rangeRawList);

        String[] logDir = getLogDir(args[1], session);

        repairDataFromParquetFileLocal(session,logDir);


    }


    private static String[] getLogDir(String month, SparkSession session) throws IOException {

        final String mainLogDir = "hdfs://192.168.23.200:9000/data/Parquet/DSPLog/";
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.23.200:9000/"),session.sparkContext().hadoopConfiguration());
        FileStatus[] ri = fs.listStatus(new Path(mainLogDir));
        List<String> paths = new ArrayList<>();
        for (FileStatus fileStatus : ri) {
            String path = fileStatus.getPath().toString();
            if (isRightDir(path,month)) {
                paths.add(path);
            }
        }
        return paths.toArray(new String[0]);
    }
    private static List<IpRange> rawIpRangeConvert(List<String> rawList) {
        List<IpRange> rangeList = new ArrayList<>();
        for (String s : rawList) {
            String[] holder = s.split(",");
            rangeList.add(new IpRange(Long.parseLong(holder[0]), Long.parseLong(holder[1])));
        }
        return rangeList;
    }

    /** kiem tra ten thu muc */

    private static boolean isRightDir(String path, String month) {

        String[] dir = path.split("/");
        String folder = dir[dir.length-1];
        String m = folder.split("_")[1];
        String y = folder.split("_")[0];
        return m.equals(month) && y.equals("2018");
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

    //  Loc du lieu tu mot file parquet tren local roi ghi du lieu ra file cvs gom co ip, city, region
    private static  void repairDataFromParquetFileLocal(SparkSession spark,String[] filePath){
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
}
