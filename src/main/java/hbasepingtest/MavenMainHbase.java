package hbasepingtest;

import org.apache.hadoop.conf.Configured;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;


import java.io.IOException;
import java.sql.*;
import java.sql.Connection;


public class MavenMainHbase extends Configured {


    /**
     * Initialization
     */
    public static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static String table_result;
    public static void main(String[] args) throws IOException, SQLException {
        System.setProperty("hive.metastore.uris","thrift://sandbox.kylo.io:9083");
        //System.setProperty("hhive.scratch.dir.permission","777");

        final SparkConf conf = new SparkConf().setAppName("SparkHive")
                .setMaster("local").setSparkHome("/usr/hdp/current/spark-client");
        conf.set("spark.yarn.dist.files","file:/usr/hdp/2.5.6.0-40/spark/conf/hive-site.xml");
        conf.set("spark.sql.hive.metastore.version","1.2.1");
        SparkContext sc = new SparkContext(conf);

         HiveContext hiveContext = new HiveContext(sc);

        hiveContext.setConf("hive.metastore.uris","thrift://sandbox.kylo.io:9083");
        sc.version();

        String db="TCGA";
        String table= "firebrowse_test_valid";

     //  DataFrame df = hiveContext.sql("USE " + db).toDF();




        new DataDictionary().generateSchemaDataDictionary(db, hiveContext, table);

        new DataDictionary().generateInstanceDataDictionary(db, hiveContext, table);


    }

    public String connectHive(String my_db, String my_table, String my_ts)
            throws SQLException {
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


        if(checktest.next()==false) {
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


}