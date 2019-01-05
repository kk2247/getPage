package kafka;

/**
 * @author KGZ
 * @date 2019/1/2 20:29
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.*;

public class PageConsumer extends Thread {
    private String groupid;
    private String topic = "page";

    public PageConsumer(String groupid){
        this.groupid = groupid;
    }

    /**
     * 进行数据爬取，将数据上传到kafka
     */
    @Override
    public void run() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "172.17.11.250:9092,172.17.11.251:9092,172.17.11.252:9092");
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("page"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        consumer.seekToBeginning(new ArrayList<TopicPartition>());

        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();
        Set<Map.Entry<String, List<PartitionInfo>>> entries = listTopics.entrySet();

        for (Map.Entry<String, List<PartitionInfo>> entry:
                entries) {
            System.out.println("topic:" + entry.getKey());

        }

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for(ConsumerRecord<String, String> record : records) {
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
                try {
                    String[] strings=record.value().split("##");
                    Put put = new Put(Bytes.toBytes(strings[1]));
                    put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(strings[0]));
                    put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(strings[1]));
                    put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("content"),Bytes.toBytes(strings[2]));
                    table.put(put);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

    }


}
