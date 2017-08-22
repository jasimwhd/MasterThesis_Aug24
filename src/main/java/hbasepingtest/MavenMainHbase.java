package hbasepingtest;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
//import org.codehaus.jackson.JsonNode;
//import org.codehaus.jackson.JsonProcessingException;
//import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class MavenMainHbase  {
    static final String REST_URL = "http://data.bioontology.org";
    static final String API_KEY = "83ec6817-48e5-434b-b087-6ea879f424a3";
    static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Initialization
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws IOException, SQLException {

        String table_result;
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "sandbox.kylo.io");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

      //  org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(conf);


        try {
            table_result= new MavenMainHbase().connectHive("firehose", "firehoseingestionsemantictest", "1502739840775");
             new MavenMainHbase().pushOntologiesData("firehose", table_result, conf );

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public String connectHive(String my_db, String my_table, String my_ts) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con1 = DriverManager.getConnection("jdbc:hive2://localhost:10000/" + my_db, "hive"
                , "hive");
        System.out.println("hello");
        Statement stmt = con1.createStatement();
        String original_table = ("describe " + my_table);
        ResultSet o_table =stmt.executeQuery(original_table);

        String col_desc="";
        while(o_table.next())
            col_desc += o_table.getString(1) + " " + o_table.getString(2) + "," ;

        col_desc =col_desc.replaceAll(",$","");

        String hbase_col_desc[] = col_desc.split(",");

        int i=0;
        String sub_query="";
        while(i<hbase_col_desc.length){
                 sub_query += "cf1:"+ hbase_col_desc[i].split(" ")[0]+",";
                    i++;
        }
        sub_query=sub_query.replaceAll(",$","");
        String sub_query_overwrite = sub_query.replaceAll("cf1:", "");

        ResultSet checktest = stmt.executeQuery("SHOW TABLES LIKE '"
                + my_table + "_temp1'");

        if(!checktest.next()) {
            String sql = "CREATE TABLE " + my_table + "_temp1" +
                    "(key string, " + col_desc +
                    ", or1_evaluation_score string, or1_pref_name string," +
                    " or1_description_id string, or1_ontology_name string, or1_ontology_id string," +
                    " or2_evaluation_score string, or2_pref_name string," +
                    " or2_description_id string, or2_ontology_name string, or2_ontology_id string," +
                    " or3_evaluation_score string, or3_pref_name string," +
                    " or3_description_id string, or3_ontology_name string, or3_ontology_id string) " +
                    "STORED BY" +
                    " 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" +
                    " WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key," +
                    "" + sub_query +
                    ", or1:evaluation_score, or1:pref_name," +
                    " or1:description_id, or1:ontology_name, or1:ontology_id," +
                    " or2:evaluation_score, or2:pref_name," +
                    " or2:description_id, or2:ontology_name, or2:ontology_id," +
                    " or3:evaluation_score, or3:pref_name," +
                    " or3:description_id, or3:ontology_name, or3:ontology_id" +
                    "\")" +
                    " TBLPROPERTIES (\"hbase.table.name\" = \"hbase_" + my_table + "_temp1\")";
            System.out.println("Running: " + sql);
            stmt.execute(sql);

            String a = "INSERT INTO TABLE " + my_table + "_temp1(" +
                 "key," + sub_query_overwrite + ") " +
                 "SELECT reflect(\"java.util.UUID\",\"randomUUID\")" +
                    " as key,* from " + my_table;

            stmt.execute(a);
        }
        return my_table + "_temp1";

        //spark optimized code to insert data here---
//
//        SparkConf conf= new SparkConf().setAppName("SparkHiveInsertionTest").setMaster("local");
//        SparkContext sc= new SparkContext(conf);
//
//        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
//
//        DataFrame df= hiveContext.sql("show firehose.tables");
//        df.show()
    }

    public void pushOntologiesData(String my_db, String my_table,  Configuration configuration) throws SQLException,IOException {

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con1 = DriverManager.getConnection("jdbc:hive2://localhost:10000/" + my_db, "hive"
                , "hive");
        System.out.println("hello");
        Statement stmt = con1.createStatement();

        ResultSet o_table = stmt.executeQuery("describe " + my_table);
        ResultSetMetaData rsmd = o_table.getMetaData();
        //   String col_name = rsmd.getColumnName(1);

        o_table.next();
        o_table.next();

        String col_name = o_table.getString(rsmd.getColumnName(1));

        ResultSet ontology_col = stmt.executeQuery("select key," + col_name + " from " + my_table);

        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection();
        TableName tableN = TableName.valueOf("hbase_"+my_table);
        Table t = conn.getTable(tableN);

        while (ontology_col.next()) {
            String rowkey = ontology_col.getString(1);
            String a = ontology_col.getString(2);
            // Get the available resources
            String resourcesString = get(REST_URL + "/recommender?input=" + a);
            JsonNode jresources = jsonToNode(resourcesString);

            // Get the name and ontology id from the returned list
            List<String> evaluation_scores = new ArrayList<String>();
            List<String> ontology_name = new ArrayList<String>();
            List<String> ontology_id = new ArrayList<String>();
            List<String> pref_name = new ArrayList<String>();
            List<String> description_id = new ArrayList<String>();

            for (JsonNode resource : jresources) {
                evaluation_scores.add(resource.get("evaluationScore").asText());
                ontology_name.add(resource.get("ontologies").
                        findValue("acronym").asText());
                ontology_id.add(resource.get("ontologies").findValue("@id").asText());
                pref_name.add(resource
                        .get("coverageResult").
                                findValue("annotations").
                                findValue("matchType").asText());

                description_id.add(resource.get("coverageResult").findValue("annotations").findValue("annotatedClass")
                        .findValue("links").findValue("self").asText());

            }
            try {
                pushintoHbase(t, rowkey, "or", "evaluation_score", evaluation_scores, configuration);
                pushintoHbase(t, rowkey, "or", "pref_name", pref_name, configuration);
                pushintoHbase(t, rowkey, "or", "description_id", description_id, configuration);
                pushintoHbase(t, rowkey, "or", "ontology_name", ontology_name, configuration);
                pushintoHbase(t, rowkey, "or", "ontology_id", ontology_id, configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
            t.close();
            conn.close();
        }



    }

    private static JsonNode jsonToNode(String json) {
        JsonNode root = null;
        try {
            root = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return root;
    }

    private static String get(String urlToGet) {
        URL url;

        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        String result = "";
        try {
            url = new URL(urlToGet);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "apikey token=" + API_KEY);
            conn.setRequestProperty("Accept", "application/json");
            rd = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void pushintoHbase(Table tablename, String rowkey, String family, String qualifier, List<String> value,  Configuration configuration) throws  Exception
    {
//        TableName tableName = TableName.valueOf(tablename);
//        Table table = connection.getTable(tableName);
       try {
//           tablename.put(new Put(rowkey.getBytes()).add(family.getBytes(),
//                   qualifier.getBytes(), value.get(0).getBytes()));
//           tablename.close();
//
//           tablename.put(new Put(rowkey.getBytes()).add(family.getBytes(),
//                   qualifier.getBytes(), value.get(1).getBytes()));
//
//           tablename.close();
//           tablename.put(new Put(rowkey.getBytes()).add(family.getBytes(),
//                   qualifier.getBytes(), value.get(2).getBytes()));
//           tablename.close();

           Put p = new Put(rowkey.getBytes());

           p.add((family+"1").getBytes(),qualifier.getBytes(),value.get(0).getBytes());
           tablename.put(p);

           p.add((family+"2").getBytes(),qualifier.getBytes(),value.get(1).getBytes());
           tablename.put(p);

           p.add((family+"3").getBytes(),qualifier.getBytes(),value.get(2).getBytes());
           tablename.put(p);


       }
       catch (IOException e)
       {e.printStackTrace();}


    }
}
