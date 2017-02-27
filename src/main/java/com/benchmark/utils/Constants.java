package com.benchmark.utils;

import java.util.Properties;

/**
 * Created by shirela on 2/6/2017.
 */
public class Constants {

    static Properties prop = Util.getProperties();

    //Threads
    public static final int NUMBER_OF_ITERATION = Integer.parseInt(prop.getProperty("number.of.iterations"));
    public static final int NUMNBER_OF_ITERATION_BULK = Integer.parseInt(prop.getProperty("number.of.iterations.bulk"));
    public static final int MAX_THREADS = Integer.parseInt(prop.getProperty("max.threads"));
    public static final int MAX_WAIT_MINUTES = Integer.parseInt(prop.getProperty("max.wait.minutes"));

    //Cassandra Details
    public static final String KEYSPACE_NAME = prop.getProperty("keyspace.name");
    public static final String CLUSTER_NAME = prop.getProperty("cluster.name");
    public static final String TABLE_NAME_TARGET = prop.getProperty("table.name.target");
    public static final String COUNTER_NAME_TARGET = prop.getProperty("counter.name.target");

    public static final String SERVER_IP = prop.getProperty("server.ip");
    public static final int SERVER_PORT = Integer.parseInt(prop.getProperty("server.port"));


}
