package hbasepingtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class DataDictionary {

    static final String REST_URL = "http://data.bioontology.org";
    static final String API_KEY = "83ec6817-48e5-434b-b087-6ea879f424a3";
    static final ObjectMapper mapper = new ObjectMapper();

    void generateSchemaDataDictionary(String db, SQLContext sqlContext, String table)
    {

        String df_query= "select * from " + db +"."+ table;
        DataFrame df = sqlContext.sql(df_query).toDF();

        StructType schema= df.schema();
        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "DD_Schema "
                + "(feed_name string, "
                + "field_name string, "
                + "description string, "
                + "preferred_type string, "
                + "preferred_label string, "
                + "ontology_uri string) ";

        DataFrame dd = sqlContext.sql(query);

       StructField[] df_struct= df.schema().fields();

        JsonNode resources;
        for (int i = 0; i < df_struct.length; i++) {
            //Get the available resources
            String resourcesString = get(REST_URL + "/recommender?input="
           + df_struct[i].name());

            resources = jsonToNode(resourcesString);
            JsonNode node= resources.get(0);
            String desc_url= node.get("coverageResult").get("annotations").get(0).get("annotatedClass").get("links").findValue("self").asText();



            // Get the ontologies from the link we found
            JsonNode desc_node = jsonToNode(get(desc_url));
            String desc=  desc_node.findValue("definition").asText();

            String pref_name = node.get("coverageResult")
                    .get("annotations")
                    .get(0)
                    .findValue("text").asText();

            String pref_type = node.get("coverageResult")
                    .get("annotations")
                    .get(0)
                    .findValue("matchType").asText();

            String ont_uri= node.get("ontologies")
                    .get(0)
                    .findValue("@id").asText();

            String DD_Schema_insert="select "+
                    "'" + table+"'" + " as feed_name, "+
                    "'" + df_struct[i].name()+"'" + " as field_name, "+
                    "'"+desc+ "'"+ " as description, "+
                    "'"+pref_type+ "'"+ " as preferred_type, "+
                    "'"+pref_name+ "'"+ " as preferred_label, " +
                    "'"+ont_uri+ "'"+ " as ontology_uri ";


            DataFrame data= sqlContext.sql(DD_Schema_insert);

            data.write().mode("append").saveAsTable(db+ "."+ "dd_schema");

            }
        }

    void generateInstanceDataDictionary(String db, SQLContext sqlContext, String table)
    {
        String df_query= "select * from " + db +"."+ table;
        DataFrame df = sqlContext.sql(df_query).toDF();

        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "DD_instance "
                + "(feed_name string, "
                + "field_name string, "
                + "field_value string"
                + "description string, "
                + "preferred_type string, "
                + "preferred_label string, "
                + "ontology_uri string) ";

        JsonNode resources;

        DataFrame dd = sqlContext.sql(query);

    //    StructField[] df_struct= df.schema().fields();
        for (int i = 0; i < df.columns().length; i++) {
            DataFrame temp= df.select(df.columns()[i]).distinct();

            //Get the available resources
            ArrayList<String> resourcesString=new ArrayList<>();
            temp.javaRDD().foreach
                    (p-> resourcesString.add(get(REST_URL + "/recommender?input=" +p.toString())));

            for (int i1 = 0; i1 < resourcesString.size(); i1++) {

                resources = jsonToNode(resourcesString.get(i1));
                JsonNode node= resources.get(0);
                String desc_url= node.get("coverageResult").get("annotations").get(0).get("annotatedClass").get("links").findValue("self").asText();



                // Get the ontologies from the link we found
                JsonNode desc_node = jsonToNode(get(desc_url));
                String desc=  desc_node.findValue("definition").asText();

                String pref_name = node.get("coverageResult")
                        .get("annotations")
                        .get(0)
                        .findValue("text").asText();

                String pref_type = node.get("coverageResult")
                        .get("annotations")
                        .get(0)
                        .findValue("matchType").asText();

                String ont_uri= node.get("ontologies")
                        .get(0)
                        .findValue("@id").asText();

                String DD_Instance_insert="select "+
                        "'" + table+"'" + " as feed_name, "+
                        "'" + df.schema().fields()[i].name()+"'" + " as field_name, "+
                        "'" + resourcesString.get(i1)+"'" + " as field_value, "+
                        "'"+desc+ "'"+ " as description, "+
                        "'"+pref_type+ "'"+ " as preferred_type, "+
                        "'"+pref_name+ "'"+ " as preferred_label, " +
                        "'"+ont_uri+ "'"+ " as ontology_uri ";


                DataFrame data= sqlContext.sql(DD_Instance_insert);

                data.write().mode("append").saveAsTable(db+ "."+ "dd_instance");
            }


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


}
