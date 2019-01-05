package spark;

import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.jruby.RubyProcess;
import scala.Tuple2;
//import scala.actors.threadpool.Arrays;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author KGZ
 * @date 2019/1/4 8:53
 */
public class DataStore extends Thread implements Serializable {

    @Override
    /**
     * 生成关键字目录和关联词目录
     */
    public void run() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.6");
        SparkConf conf=new SparkConf();
        conf.setAppName("fetchData");
        conf.setMaster("spark://172.17.11.246:7077");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("error");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");

        while(true){
            List<String> list=readHbase();
            list=pastLeep(list);
            JavaRDD<String> rdd1=sc.parallelize(list);
            JavaRDD<String> getkey=rdd1.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String s) throws Exception {
                    JiebaSegmenter segmenter = new JiebaSegmenter();
                    String[] strings=s.split("##");
                    String titile=strings[1].trim();
                    List<String> list1=new ArrayList<String>();
                    List<String> strs=segmenter.sentenceProcess(titile);
                    for(int i=0;i<strs.size();i++){
                        if(strs.get(i).replace(" ","").replace(".","").length()<2 || strs.get(i).contains("-") || strs.get(i).contains(">")
                                || strs.get(i).contains("…") || strs.get(i).contains("》") || strs.get(i).contains("(") || strs.get(i).contains(")")
                                || strs.get(i).contains("?") ||  strs.get(i).contains("—")||  strs.get(i).contains("=")||  strs.get(i).contains("+")
                                ||  strs.get(i).contains("\"")){
                            strs.remove(i);
                            i--;
                        }else{
                            Pattern pattern = Pattern.compile("[0-9]*");
                            Matcher isNum = pattern.matcher(strs.get(i));
                            if(isNum.matches()){
                                strs.remove(i);
                                i--;
                            }else{
                                list1.add(strs.get(i)+" ## "+s);
                            }
                        }
                    }
                    return list1.iterator();
                }
            });

            JavaPairRDD<String,Integer> wordsMap = getkey.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2(s,1);
                }
            });

            JavaPairRDD<String,Integer> wordsReduce = wordsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer arg0, Integer arg1) throws Exception {
                    return arg0 + arg1;
                }
            });

            Table table=null;
            Connection connection =null;
            try {
                connection = ConnectionFactory.createConnection(config);
                table = connection.getTable(TableName.valueOf("search"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            List<String> keyword=new ArrayList<>();
            List<Tuple2<String, Integer>> output = wordsReduce.collect();
            int i=1;
            for (Tuple2<?, ?> tuple : output) {

                String con=String.valueOf(tuple._1());
                String[] strings=con.split("##");
                String key=strings[0];
                String url=strings[1];
                String title=strings[2];
                String content=strings[3];
                Put put = new Put(Bytes.toBytes("row"+i));
                put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(url));
                put.addColumn(Bytes.toBytes("key"),Bytes.toBytes("key"),Bytes.toBytes(key));
                put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(title));
                put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("content"),Bytes.toBytes(content));
                try {
                    table.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                keyword.add(key);
                System.out.println(tuple._1());
                i++;
            }
            keyword=pastLeep(keyword);
            for(String top:keyword){
                getLink(top.trim(),sc,connection);
            }
            System.out.println();
            try {
                System.out.println("ok");
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<String> readHbase(){
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.6");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");
        List<String> list=new ArrayList<String>();
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf("page"));
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                String content=new String(CellUtil.cloneValue(result.rawCells()[0]),"utf-8");
                String title=new String(CellUtil.cloneValue(result.rawCells()[1]),"utf-8");
                String url=new String(CellUtil.cloneValue(result.rawCells()[2]),"utf-8");
                String con = url+" ## "+title+" ## "+content;
                list.add(con);
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public List<String> pastLeep(List<String> list){
        System.out.println("list = [" + list + "]");
        List<String> listNew=new ArrayList<>(new TreeSet<String>(list));
        return listNew;
    }

    public List<String> getLink(String word,JavaSparkContext sc,Connection connection){
        List<String> top=new ArrayList<>();
        try {
            Table table = connection.getTable(TableName.valueOf("search"));
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            List<String> link=new ArrayList<>();
            for (Result result : results) {
                String content=new String(CellUtil.cloneValue(result.rawCells()[0]),"utf-8").trim();
                String key=Bytes.toString(CellUtil.cloneValue(result.rawCells()[1])).trim();
                if(key.equals(word)){
                    JiebaSegmenter segmenter = new JiebaSegmenter();
                    List<String> strs=segmenter.sentenceProcess(content);
                    for(int i=0;i<strs.size();i++){
                        if(strs.get(i).replace(" ","").replace(".","").length()<2 || strs.get(i).contains("-") || strs.get(i).contains(">")
                                || strs.get(i).contains("…") || strs.get(i).contains("》") || strs.get(i).contains("(") || strs.get(i).contains(")")
                                || strs.get(i).contains("?") ||  strs.get(i).contains("—")){
                            strs.remove(i);
                            i--;
                        }else{
                            Pattern pattern = Pattern.compile("[0-9]*");
                            Matcher isNum = pattern.matcher(strs.get(i));
                            if(isNum.matches()){
                                strs.remove(i);
                                i--;
                            }else{
                                link.add(strs.get(i));
                            }

                        }
                    }
                }
            }
            JavaRDD<String> list1 = sc.parallelize(link);
            JavaRDD<String> words = list1.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String s) throws Exception {
                    List<String>list2=new ArrayList<>();
                    list2.add(s);
                    return list2.iterator();
                }
            });
            JavaPairRDD<String,Integer> wordsMap = words.mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2(s,1);
                }
            });
            JavaPairRDD<String,Integer> wordsReduce = wordsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer arg0, Integer arg1) throws Exception {
                    return arg0 + arg1;
                }
            });
            List<Tuple2<String, Integer>> output=wordsReduce.mapToPair(s -> new Tuple2<Integer, String>(s._2, s._1))
                    .sortByKey(false)
                    .mapToPair(s -> new Tuple2<String, Integer>(s._2, s._1))
                    .collect();
            String linkWord="";
            for (int i=0;i<10;i++) {
                top.add(output.get(i)._1().trim());
                if(i==9){
                    linkWord=linkWord+output.get(i)._1().trim();
                }else{
                    linkWord=linkWord+output.get(i)._1().trim()+" ## ";
                }
            }
            Table table1 = connection.getTable(TableName.valueOf("key"));
            Put put = new Put(Bytes.toBytes(word));
            put.addColumn(Bytes.toBytes("link"),Bytes.toBytes("link"),Bytes.toBytes(linkWord));
            put.addColumn(Bytes.toBytes("word"),Bytes.toBytes("word"),Bytes.toBytes(word));
            table1.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return top;
    }
}
