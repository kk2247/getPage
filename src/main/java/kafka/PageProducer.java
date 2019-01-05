package kafka;

/**
 * @author KGZ
 * @date 2019/1/2 19:57
 */

import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.producer.*;
import page.GetPage;
import page.PageContent;

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class PageProducer extends Thread{

    private GetPage getPage=new GetPage();

    /**
     * kafka消费者将数据存入hbase中进行持久化。
     */
    @Override
    public void run() {
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "172.17.11.250:9092,172.17.11.251:9092,172.17.11.252:9092");
            props.put("acks", "0");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.6");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            while(true) {
                PageContent pageContent=getPage.getUrl();
                String pageInfo=pageContent.getUrl()+" ## "+pageContent.getTitle()+" ## "+pageContent.getContent();
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("page",pageContent.getTitle() ,pageInfo);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                    }
                });

                System.out.println("message send to partition " + pageContent.getTitle()+":"+pageContent.getUrl());

                Thread.sleep(10000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
