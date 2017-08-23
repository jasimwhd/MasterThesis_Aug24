package hbasepingtest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static hbasepingtest.MavenMainHbase.table_result;

public class BufferedMutatorHbaseFill extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(BufferedMutatorHbaseFill.class);

    private static final int POOL_SIZE = 50;
    private static final int TASK_COUNT = 100;
    private static final TableName TABLE = TableName.valueOf(MavenMainHbase.table_result);

    private static final byte[] FAMILY1 = Bytes.toBytes("or1");
    private static final byte[] FAMILY2 = Bytes.toBytes("or2");
    private static final byte[] FAMILY3 = Bytes.toBytes("or3");
//
//    static final String REST_URL = "http://data.bioontology.org";
//    static final String API_KEY = "83ec6817-48e5-434b-b087-6ea879f424a3";
//    static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public int run(String[] args)
            throws InterruptedException,
            ExecutionException, TimeoutException, SQLException, IOException
    {

        /** a callback invoked when an asynchronous write fails. */
        final BufferedMutator.ExceptionListener listener =
                new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TABLE)
                .listener(listener);

        //
        // step 1: create a single Connection and a BufferedMutator, shared by all worker threads.
        //
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "sandbox.kylo.io");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        try (final Connection conn = ConnectionFactory.createConnection(conf);
             final BufferedMutator mutator = conn.getBufferedMutator(params)) {

            //ontology code
            Table table = conn.getTable(TABLE);
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes(ListHbaseTablesMetadata.getColumn(args[0],args[1]).get(0)));
            ResultScanner scanner = table.getScanner(s);
            int k=0;
            for (Result ontology_col : scanner) {
                String rowkey = Bytes.toString(ontology_col.getRow());
                List<Cell> a = ontology_col.listCells();

                String input="";
                for(Cell cell: a)
                    input += Bytes.toString(CellUtil.cloneValue(cell));
                // Get the available resources
                String resourcesString = get
                        (MavenMainHbase.REST_URL + "/recommender?input=" + input);
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
                              //code ends here


            /** worker pool that operates on BufferedTable instances */
            final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        //
                        // step 2: each worker sends edits to the shared BufferedMutator instance. They all use
                        // the same backing buffer, call-back "listener", and RPC executor pool.
                        //
                        Put p = new Put(Bytes.toBytes(rowkey));
                        p.addColumn(FAMILY1, Bytes.toBytes("evaluation_score"), Bytes.toBytes(evaluation_scores.get(0)));
                        p.addColumn(FAMILY1, Bytes.toBytes("pref_name"), Bytes.toBytes(pref_name.get(0)));
                        p.addColumn(FAMILY1, Bytes.toBytes("description_id"), Bytes.toBytes(description_id.get(0)));
                        p.addColumn(FAMILY1, Bytes.toBytes("ontology_name"), Bytes.toBytes(ontology_name.get(0)));
                        p.addColumn(FAMILY1, Bytes.toBytes("ontology_id"), Bytes.toBytes(ontology_id.get(0)));

                        p.addColumn(FAMILY2, Bytes.toBytes("evaluation_score"), Bytes.toBytes(evaluation_scores.get(1)));
                        p.addColumn(FAMILY2, Bytes.toBytes("pref_name"), Bytes.toBytes(pref_name.get(1)));
                        p.addColumn(FAMILY2, Bytes.toBytes("description_id"), Bytes.toBytes(description_id.get(1)));
                        p.addColumn(FAMILY2, Bytes.toBytes("ontology_name"), Bytes.toBytes(ontology_name.get(1)));
                        p.addColumn(FAMILY2
                                , Bytes.toBytes("ontology_id"), Bytes.toBytes(ontology_id.get(1)));

                        p.addColumn(FAMILY3, Bytes.toBytes("evaluation_score"), Bytes.toBytes(evaluation_scores.get(2)));
                        p.addColumn(FAMILY3, Bytes.toBytes("pref_name"), Bytes.toBytes(pref_name.get(2)));
                        p.addColumn(FAMILY3, Bytes.toBytes("description_id"), Bytes.toBytes(description_id.get(2)));
                        p.addColumn(FAMILY3, Bytes.toBytes("ontology_name"), Bytes.toBytes(ontology_name.get(2)));
                        p.addColumn(FAMILY3, Bytes.toBytes("ontology_id"), Bytes.toBytes(ontology_id.get(2)));

                        mutator.mutate(p);
                        mutator.flush();
                        // do work... maybe you want to call mutator.flush() after many edits to ensure any of
                        // this worker's edits are sent before exiting the Callable
                        return null;
                    }
                }));
            }

            //
            // step 3: clean up the worker pool, shut down.
            //
            for (Future<Void> f : futures) {
                f.get(5, TimeUnit.MINUTES);
            }
            workerPool.shutdown();
            }
        }
        catch (IOException e) {
            // exception while creating/destroying Connection or BufferedMutator
            LOG.info("exception while creating/destroying Connection or BufferedMutator", e);
        } // BufferedMutator.close() ensures all work is flushed. Could be the custom listener is
        // invoked from here.
        return 0;
    }

    private static JsonNode jsonToNode(String json) {
        JsonNode root = null;
        try {
            root = MavenMainHbase
            .mapper.readTree(json);
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
            conn.setRequestProperty("Authorization", "apikey token=" + MavenMainHbase.API_KEY);
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
