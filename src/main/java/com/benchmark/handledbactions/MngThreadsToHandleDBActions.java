package com.benchmark.handledbactions;


import com.benchmark.utils.Constants;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by shirela on 2/6/2017.
 * <p>
 * A bean that is responsible for do the job for single record
 */
public class MngThreadsToHandleDBActions {
    public Cluster cluster;
    public Session session;

    //public static AtomicInteger atomicInteger = new AtomicInteger(0);
    public AtomicInteger atomicInteger = new AtomicInteger(0);

    public MngThreadsToHandleDBActions() {
    }




    public void mngThreadsToHandleAction() {
        String nodes = Constants.SERVER_IP;

        //checking for 75 - run as no cluster
        //String nodes = "192.168.10.75";

        cluster = connect(nodes,Constants.SERVER_PORT, Constants.CLUSTER_NAME );
        Session session = cluster.connect();
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(Constants.MAX_THREADS);
            for (int i = 0; i < Constants.NUMBER_OF_ITERATION; i++) {
                executor.execute(new DoTheJob(cluster, session , Constants.SERVER_IP,Constants.KEYSPACE_NAME,Constants.TABLE_NAME_TARGET,Constants.COUNTER_NAME_TARGET,Constants.NUMNBER_OF_ITERATION_BULK, atomicInteger));
            }

        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }


        try {
            if (executor.awaitTermination(Constants.MAX_WAIT_MINUTES, TimeUnit.MINUTES)) {
            } else {
                System.out.println("The task was unable to complete in {} minutes" +  Constants.MAX_WAIT_MINUTES);
            }
        } catch (InterruptedException e) {
            System.out.println("The task was interrupted\n" +  e);

            System.out.println("The task was interrupted\n" +  e);
        }


    }

    protected static Cluster connect(final String node, final int port, final String clusterName) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);

        final Cluster cluster = Cluster.builder()
                .addContactPoints(node.split(","))
                .withPort(port)
                .withPoolingOptions(poolingOptions)
                //.withClusterName(clusterName+"70")
                .withClusterName(clusterName)
                //.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy()) //uses the DC of the seed node it connects to!! So one needs to give it the right seed
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .build();
        final Metadata metadata = cluster.getMetadata();
        return cluster;
    }


}
