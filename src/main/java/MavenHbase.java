import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class MavenHbase {
    static final String REST_URL = "http://data.bioontology.org";
    static final String API_KEY = "";
    static final ObjectMapper mapper = new ObjectMapper();
    // private static Configuration conf = null;

    /**
     * Initialization
     */


//
    public static void main(String[] args) throws IOException, ServiceException {

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-default.xml"));
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName table =null;
        try {
            table = TableName.valueOf("sample");
            Admin admin = connection.getAdmin();


            HTableDescriptor desc = new HTableDescriptor(table);
            desc.addFamily(new HColumnDescriptor("family1"));
            admin.createTable(desc);
//            desc.addFamily(new HColumnDescriptor())

//            byte[] key = Bytes.toBytes("row-by-java-client");
//            byte[] val = Bytes.toBytes("val");
//
//
//            Put p = new Put(key);
//            byte[] family = Bytes.toBytes("data");
//            byte[] column = Bytes.toBytes("column");
//            p.addColumn(family, column, val);
//            table.put(p);

//            // 2) データの取得
//            Get g = new Get(key);
//            Result r = table.get(g);
//            System.out.println("Get: " + r);
//
//            // 3) データのスキャン
//            Scan scan = new Scan();
//            ResultScanner scanner = table.getScanner(scan);
//            try {
//                // Scannerの結果をイテレート
//                for (Result sr : scanner)
//                    System.out.println("Scan: " + sr);
//            } finally {
//                scanner.close();
//            }
//
//            // 4) 特定のrowからのscan
//            byte[] start = Bytes.toBytes("row3");
//            scan = new Scan(start);
//            scanner = table.getScanner(scan);
//            try {
//                // Scannerの結果をイテレート
//                for (Result sr : scanner)
//                    System.out.println("Scan: " + sr);
//            } finally {
//                scanner.close();
//            }
        }
        finally {
            connection.close();
        }
//
//        HBaseClientOperations HBaseClientOperations = new HBaseClientOperations();
//        HBaseClientOperations.run(config);
    }
}
