package frontKafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.*;
import page.PageContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author KGZ
 * @date 2019/1/4 10:25
 */
public class PullData extends Thread{

    public void pull(){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            List<String> key=queryKey(connection);
            List<String> search=querySearch(connection);

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> queryKey(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("key"));
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        ArrayList<String> list=new ArrayList<>();
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                String link=Bytes.toString(CellUtil.cloneValue(result.rawCells()[0])).trim();
                String word=Bytes.toString(CellUtil.cloneValue(result.rawCells()[1])).trim();
                String string=word +" ## "+link;
                list.add(string);
            }
        }
        return list;
    }

    private List<String> querySearch(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("search"));
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        ArrayList<String> list=new ArrayList<>();
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                String content=Bytes.toString(CellUtil.cloneValue(result.rawCells()[0])).trim();
                String key=Bytes.toString(CellUtil.cloneValue(result.rawCells()[1])).trim();
                String title=Bytes.toString(CellUtil.cloneValue(result.rawCells()[2])).trim();
                String url=Bytes.toString(CellUtil.cloneValue(result.rawCells()[3])).trim();
                String string=key +" ## "+url+" ## "+title;
                list.add(string);
            }
        }
        return list;
    }
}
