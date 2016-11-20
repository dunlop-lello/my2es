package uk.dunloplello.db2es.my2es;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.AbstractLifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetColumnPacket;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import static org.elasticsearch.client.Requests.indexRequest;

public class App {

    TransportClient transportClient;
    BinaryLogClient binlogClient;

    protected ArrayList<String> schemas() throws IOException {
        final ArrayList<String> result = new ArrayList();

        binlogClient.execute("select DISTINCT TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES", new BinaryLogClient.AbstractResultSetListener() {
            public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                result.add(resultSetRowPacket.getValue(0));
            }
        });
        return result;
    }

    protected ArrayList<String> tables(String database) throws IOException {
        final ArrayList<String> result = new ArrayList();

        binlogClient.execute("select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE='BASE TABLE' and TABLE_SCHEMA='"+database+"'", new BinaryLogClient.AbstractResultSetListener() {
            public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                result.add(resultSetRowPacket.getValue(0));
            }
        });
        return result;
    }
    protected App(String[] args) throws Exception {
        transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        System.out.println(transportClient.settings().toDelimitedString(','));
        Thread.sleep(10000);
        if (!transportClient.admin().indices().prepareExists("mysql").get().isExists()) {
            transportClient.admin().indices().prepareCreate("mysql").get();
        }
        binlogClient = new BinaryLogClient("172.17.0.2", 3306, "root", "");
        binlogClient.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                switch (event.getHeader().getEventType()) {
                    case QUERY:
                        System.out.println(event.toString());
                        break;                        
                }
            }
        });
        binlogClient.registerLifecycleListener(new AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                try {
                    System.out.println(this.getClass().getName() + " onConnect");
                    for (final String schema : schemas()) {
                        switch (schema) {
                            case "information_schema":
                            case "mysql":
                            case "performance_schema":
                            case "sys":
                                continue;
                            default:
                                if (false) {
                                System.out.println(schema+": "+tables(schema));
                                for (final String table : tables(schema)) {
                                    final TableSchema ts = new TableSchema(schema, table);
                                    System.out.println(ts);
                                    HashMap<String,Object> source = new HashMap();
                                    for (Map<String,Object> column : ts.columns()) {
                                        source.put((String)column.get("COLUMN_NAME"), column.get("DATA_TYPE"));
                                    }
                                    System.out.println(transportClient.prepareIndex("mysql", schema+"."+table).setSource(source).get().toString());
                                    binlogClient.execute("SELECT * FROM "+schema+"."+table, new BinaryLogClient.AbstractResultSetListener() {
                                        BulkRequestBuilder bulkRequest;
                                        
                                        public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                                            IndexRequest request = indexRequest("mysql");
                                            
                                            if (bulkRequest == null) {
                                                bulkRequest = transportClient.prepareBulk();
                                            }
                                            Map<String,Object> source = new HashMap();
                                            int i = 0;
                                            for (Map<String,Object> column : ts.columns()) {
                                                String k = (String)column.get("COLUMN_NAME");
                                                Object v = resultSetRowPacket.getValue(i);
                                                source.put(k, v);
                                                
                                                if (k.equals(ts.primarykey())) {
                                                    request.id(v.toString());
                                                }
                                                i++;
                                            }
                                            request.type(schema+"."+table).source(source);
                                            bulkRequest.add(request);
                                            if (bulkRequest.numberOfActions() >= 10000)
                                            {
                                                for (BulkItemResponse item : bulkRequest.get().getItems()) {
                                                    System.out.println(item.getResponse());                                                    
                                                }
                                                bulkRequest = null;
                                            }
                                        }

                                        @Override
                                        public void afterResultSetRowPackets() {
                                            if (bulkRequest != null)
                                            {
                                                for (BulkItemResponse item : bulkRequest.get().getItems()) {
                                                    System.out.println(item.getResponse());                                                    
                                                }                                                System.out.println(bulkRequest.get().toString());
                                            }
                                        }
                                    });
                                }
                                }
                                break;
                        }
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        binlogClient.setBinlogFilename("mysql-bin.000001");
        binlogClient.setBinlogPosition(4);
        binlogClient.connect();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("log4j2.disable.jmx", Boolean.TRUE.toString());

        App app = new App(args);
    }
    
    class TableSchema {
        final protected List<Map<String, Object>> columns;
        protected String primaryKey;
        
        TableSchema(String database, String table) throws IOException {
            String sql;
            final List<Map<String, Object>> rows = new ArrayList();

            sql = "select * from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='"+database+"' AND TABLE_NAME='"+table+"'";
            binlogClient.execute(sql, new BinaryLogClient.AbstractResultSetListener() {
                List<ResultSetColumnPacket> columns;

                @Override
                public void beforeResultSetRowPackets(ArrayList<ResultSetColumnPacket> columns) {
                    this.columns = columns;
                }
                
                public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                    HashMap<String, Object> row = new HashMap();
                    for (int column = 0; column < columns.size(); column++) {
                        String k = columns.get(column).name();
                        Object v = resultSetRowPacket.getValue(column);
                        
                        row.put(k, v);
                    }
                    rows.add(Collections.unmodifiableMap(row));
                }
            });
            columns = Collections.unmodifiableList(rows);

            sql = "select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA='"+database+"' and TABLE_NAME='"+table+"'";
            binlogClient.execute(sql, new BinaryLogClient.AbstractResultSetListener() {
                
                public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                    primaryKey = resultSetRowPacket.getValue(0);
                }
            });
        }

        public List<Map<String,Object>> columns() {
            return columns;
        }
        
        public String primarykey() {
            return primaryKey;
        }
        
        @Override
        public String toString() {
            return "TableSchema{" + "columns=" + columns + '}';
        }
    }
}
