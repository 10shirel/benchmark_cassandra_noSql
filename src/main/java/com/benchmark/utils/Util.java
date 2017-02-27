package com.benchmark.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by shirela on 2/6/2017.
 */
public class Util {

    public static Properties getProperties() {
        String resourceName = "application.properties"; // could also be a constant
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        Properties props = new Properties();
        try {
            try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
                props.load(resourceStream);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

/*    public static Properties getProperties() {

        Properties props = new Properties();
        try {
        props.load(Util.class.getResourceAsStream("/application2.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }*/

    public static int count()

    {   Cluster cluster = Cluster.builder()
            .addContactPoints(Constants.SERVER_IP)
            .build();

        Session session = cluster.connect(Constants.KEYSPACE_NAME);

        String cqlStatement ="select count(*) from " + Constants.KEYSPACE_NAME + "." + Constants.TABLE_NAME_TARGET + ";";
        ResultSet resultSet = session.execute(cqlStatement);
        return 1;

    }
}
