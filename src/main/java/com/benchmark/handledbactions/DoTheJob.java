package com.benchmark.handledbactions;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by shirela on 2/6/2017.
 */
public class DoTheJob implements Runnable {

    private String ip;
    private String keySpace;
    private String targetTableName;
    private String counterTableName;
    private int numberOfIterationBulk;
    private AtomicInteger atomicInteger;
    private Session session;
    private Cluster cluster;

    public DoTheJob(Cluster cluster, Session session, String ip, String keySpace, String targetTableName, String counterTableName, int numberOfIterationBulk, AtomicInteger atomicInteger) {
        this.ip = ip;
        this.keySpace = keySpace;
        this.targetTableName = targetTableName;
        this.counterTableName = counterTableName;
        this.numberOfIterationBulk = numberOfIterationBulk;
        this.atomicInteger = atomicInteger;
        this.session = session;
        this.cluster = cluster;
    }


    public void run() {
        System.out.println(Thread.currentThread().getId());
        insertToDB();
    }

    /**
     * Insert records to DB
     */
    private void insertToDB() {
        insertToDBAllFlow(session);
    }


    public void insertToDBAllFlow(Session session) {

        String cqlStatement =
                "INSERT INTO " + keySpace + "." + targetTableName +
                        "( a1_transaction_id, a2_source, a3_target, a4_message," +
                        "f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17," +
                        "f18, f19, f20, f21, f22, f23, f24, f25, f26, f27, f28, f29, f30)" +
                        "VALUES ( now() , 'source V', 'target V', 'messag V', " +
                        " 'V6', 'V7', 'V8', 'V9', 'V10', 'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17'," +
                        " 'V18', 'V19', 'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28', 'V29', 'V30');";

/*
        String cqlStatementUpdateCounter =
                "UPDATE " + keySpace + "." + counterTableName +
                        " SET counter_value = counter_value +1" +
                        " WHERE insert_to_table=" + "'" + targetTableName + "';";
*/
        String cqlStatementUpdateCounter =
                "UPDATE " + keySpace + "." + counterTableName +
                        " SET counter_value = counter_value +" + numberOfIterationBulk/50 +
                        " WHERE insert_to_table=" + "'" + targetTableName + "';";

        String cqlStatementCount = "select counter_value from " + keySpace + ".page_view_counts;";


        try {
            for (int j = 0; j < 50; j++) {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
            for (int i = 0; i < (numberOfIterationBulk/50); i++) {
                    final LoadBalancingPolicy loadBalancingPolicy =
                            session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
                    final PoolingOptions poolingOptions =
                            session.getCluster().getConfiguration().getPoolingOptions();
                    Session.State state = session.getState();
                    for (Host host : state.getConnectedHosts()) {
                        HostDistance distance = loadBalancingPolicy.distance(host);
                        int connections = state.getOpenConnections(host);
                        int inFlightQueries = state.getInFlightQueries(host);
                        System.out.printf("%s connections=%d, current load=%d, max load=%d%n",
                                host, connections, inFlightQueries,
                                connections *
                                        poolingOptions.getMaxRequestsPerConnection(distance));
                    }


                    // add bound statements to the batch
                    batch.add(new SimpleStatement(cqlStatement));
                }
            session.execute(batch);
            session.execute(new SimpleStatement(cqlStatementUpdateCounter));
            }

        } catch (OperationTimedOutException e) {
            System.out.println("OperationTimedOutException" + e.getCause());
        } catch (ConnectionException e) {
            System.out.println(e.getCause());
        } catch (ServerError e) {
            System.out.println("ServerError" + e.getCause());
        } catch (Exception e) {
            System.out.println("threadId-" + Thread.currentThread().getId() + " atomicInteger = " + atomicInteger.get() + e.getMessage());
        } catch (Throwable e) {
            System.out.println("threadId-" + Thread.currentThread().getId() + " atomicInteger = " + atomicInteger.get() + e.getMessage());
        }


    }

}
