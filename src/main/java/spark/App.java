package spark;

import model.IpRange;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;


import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Hello world!
 *
 */

public class App {
    private static List<Long> ipLeft;
    private static List<IpRange> rangeList;
    private static Map<Long, Integer> ipMap;

    public static void main( String[] args ) throws Exception {

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
         */

        ipLeft = new ArrayList<>();
        Dataset<String> rangeDS = session.read().textFile(args[0]);
        List<String> rangeRawList = rangeDS.collectAsList();
        rangeList = rawIpRangeConvert(rangeRawList);

        String[] logDir = getLogDir(args[1], session);

        Dataset<Row> logDS = session.read().parquet(logDir);
        Dataset<Long> ipDS = logDS.select("ip").as(Encoders.LONG());
        JavaRDD<Long> allIPDataset = ipDS.javaRDD();

        JavaPairRDD<Long, Integer> ipPairRDD = allIPDataset
                .mapToPair(ip -> new Tuple2<>(ip, 1))
                .reduceByKey((a,b) -> a+b);

        long allRow = ipDS.count();
        System.out.println("All rows : " + allRow);
        ipMap = ipPairRDD.collectAsMap();
        double cover = checkIpCover(allRow, ipDS.distinct().collectAsList());
        System.out.println(allRow+"\n"+cover*100);

        if (args.length == 3 && args[2].equals("true")) {
            JavaSparkContext jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
            JavaRDD<Long> ipLeftRDD = jsc.parallelize(ipLeft);
            ipLeftRDD.saveAsTextFile("unknownIP");
        }

        if (args.length == 4) {
            logDS.show(Integer.parseInt(args[3]), 100);
        }

        session.stop();
    }
    /** get cac thu muc cung 1 thang */

    private static String[] getLogDir(String month, SparkSession session) throws IOException {

        final String mainLogDir = "hdfs://192.168.23.200:9000/data/Parquet/PageViewV1";
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

    private static double checkIpCover(long allRow,List<Long> allIPList) {
        long cover = 0;

        for (Long ip : allIPList) {
            if (isInList(ip)) {
                if (ipMap.get(ip) != null) cover +=ipMap.get(ip);
            } else {
                ipLeft.add(ip);
            }
        }

        System.out.println("Not in Location : " + ipLeft.size());
        System.out.println("Row with location detected : " + cover);
        return (double) cover /allRow;
    }

    /** kiem tra ip co trong danh sach ip location */

    private static boolean isInList(Long ip) {
        for (IpRange ipRange : rangeList) {
            if (ipRange.isInRange(ip)) return true;
        }
        return false;
    }

    /** chuyen tu dang text -> IpRange */

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
}
