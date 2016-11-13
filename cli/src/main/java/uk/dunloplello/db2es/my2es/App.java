package uk.dunloplello.db2es.my2es;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.AbstractLifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetColumnPacket;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

public class App
{
    private final Logger logger = Logger.getLogger(getClass().getName());

    public App( String[] args ) throws Exception {
        BinaryLogClient client = new BinaryLogClient("172.17.0.2", 3306, "root", "");
        client.registerEventListener(new BinaryLogClient.EventListener() {

            @Override
            public void onEvent(Event event) {
                logger.info(event.toString());
            }
        });
        client.registerLifecycleListener(new AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                try {
                    logger.info(this.getClass().getName() + " onConnect");
                    client.execute("select DISTINCT TABLE_SCHEMA,TABLE_CATALOG from INFORMATION_SCHEMA.TABLES", new BinaryLogClient.AbstractResultSetListener() {
                        public void beforeResultSetRowPackets(ArrayList<ResultSetColumnPacket> columns) {
                            for (ResultSetColumnPacket column : columns) {
                                logger.info(column.toString());
                            }
                        }

                        public void onResultSetRowPacket(ResultSetRowPacket resultSetRowPacket) {
                            logger.info(Arrays.toString(resultSetRowPacket.getValues()));
                        }
                        
                        public void afterResultSetRowPackets() {
                            logger.info("Done");
                        }
                    });
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
        client.connect();
    }
    
    public static void main( String[] args ) throws Exception
    {
        App app = new App(args);
    }
}
