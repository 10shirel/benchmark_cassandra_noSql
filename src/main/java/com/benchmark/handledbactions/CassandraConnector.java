package com.benchmark.handledbactions;

import com.benchmark.utils.Constants;
import com.datastax.driver.core.*;

import java.util.Collections;

import static java.lang.System.out;






public class CassandraConnector {


    public static void main(final String[] args)
    {
        final CassandraConnector client = new CassandraConnector();
        final String ipAddress = args.length > 0 ? args[0] : "192.168.10.70"/*"localhost"*/;
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042/*9160*/;
        out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
        client.connect(ipAddress, port);
        client.close();
    }




    /**
     * Cassandra Cluster.
     */

    private Cluster cluster;

    /**
     * Cassandra Session.
     */

    private Session session;

    /**
     * Connect to Cassandra Cluster specified by provided node IP
     * <p>
     * address and port number.
     *
     * @param node Cluster node IP address.
     * @param port Port of cluster host.
     */

    public void connect(final String node, final int port)

    {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).withClusterName("Test Cluster").build();
        final Metadata metadata = cluster.getMetadata();

        out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (final Host host : metadata.getAllHosts())

        {

            out.printf("Datacenter: %s; Host: %s; Rack: %s\n",

                    host.getDatacenter(), host.getAddress(), host.getRack());

        }

        session = cluster.connect();

    }


    /**
     * Provide my Session.
     *
     * @return My session.
     */
    public Session getSession() {
        return this.session;
    }

    /**
     * Close cluster.
     */
    public void close() {
        cluster.close();
    }
}