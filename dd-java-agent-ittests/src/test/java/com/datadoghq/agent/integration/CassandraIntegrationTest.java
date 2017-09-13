package com.datadoghq.agent.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by gpolaert on 6/2/17. */
public class CassandraIntegrationTest {

  @Before
  public void start()
      throws InterruptedException, TTransportException, ConfigurationException, IOException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(40000L);
  }

  @After
  public void stop() {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Test
  public void testNewSessionSync() throws ClassNotFoundException {
    final Cluster cluster = EmbeddedCassandraServerHelper.getCluster();
    final Session session = cluster.newSession();
    assertThat(session.getClass().getName()).endsWith("contrib.cassandra.TracingSession");
  }

  @Test
  public void testNewSessionAsync()
      throws ClassNotFoundException, ExecutionException, InterruptedException {
    final Cluster cluster = EmbeddedCassandraServerHelper.getCluster();
    final Session session = cluster.connectAsync().get();
    assertThat(session.getClass().getName()).endsWith("contrib.cassandra.TracingSession");
  }
}