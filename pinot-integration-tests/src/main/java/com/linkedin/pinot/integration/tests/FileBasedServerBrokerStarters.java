package com.linkedin.pinot.integration.tests;

import java.util.Iterator;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.linkedin.pinot.broker.broker.BrokerServerBuilder;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 12, 2014
 */

public class FileBasedServerBrokerStarters {

  private static final Logger logger = Logger.getLogger(FileBasedServerBrokerStarters.class);
  private ServerInstance serverInstance;
  private BrokerServerBuilder bld;

  /*
   * Constants :
   * change these if you need to
   * */

  // common properties
  public static final String[] RESOURCE_NAMES = { "resource1", "resource2" };

  // server properties
  public static final String SERVER_PORT = "8000";
  public static final String SERVER_INDEX_DIR = "/tmp/index";
  public static final String SERVER_BOOTSTRAP_DIR = "/tmp/boostrap";
  public static final String SERVER_INDEX_READ_MODE = "heap";
  // broker properties
  public static final String BROKER_CLIENT_PORT = "8001";

  public static final String prefix = "pinot.server.instance.";

  public FileBasedServerBrokerStarters() {

  }

  private String getKey(String... keys) {
    return StringUtils.join(keys, ".");
  }

  private PropertiesConfiguration brokerProperties() {
    final PropertiesConfiguration brokerConfiguration = new PropertiesConfiguration();

    //transport properties

    //config based routing
    brokerConfiguration.addProperty("pinot.broker.transport.routingMode", "CONFIG");

    //two resources
    brokerConfiguration.addProperty("pinot.broker.transport.routing.resourceName", StringUtils.join(RESOURCE_NAMES, ","));

    // resource a conf

    for (final String resource : RESOURCE_NAMES) {
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", resource, "numNodesPerReplica"), "1");
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", resource, "serversForNode.0"), "localhost:" + SERVER_PORT);
      brokerConfiguration.addProperty(getKey("pinot.broker.transport.routing", resource, "serversForNode.default"), "localhost:"
          + SERVER_PORT);
    }
    // client properties
    brokerConfiguration.addProperty("pinot.broker.client.enableConsole", "true");
    brokerConfiguration.addProperty("pinot.broker.client.queryPort", BROKER_CLIENT_PORT);
    brokerConfiguration.addProperty("pinot.broker.client.consolePath", "dont/need/this");
    brokerConfiguration.setDelimiterParsingDisabled(false);
    return brokerConfiguration;
  }

  private PropertiesConfiguration serverProperties() {
    final PropertiesConfiguration serverConfiguration = new PropertiesConfiguration();
    serverConfiguration.addProperty(getKey("pinot.server.instance", "id"), "0");
    serverConfiguration.addProperty(getKey("pinot.server.instance", "bootstrap.segment.dir"), SERVER_BOOTSTRAP_DIR);
    serverConfiguration.addProperty(getKey("pinot.server.instance", "dataDir"), SERVER_INDEX_DIR);
    serverConfiguration.addProperty(getKey("pinot.server.instance", "resourceName"), StringUtils.join(RESOURCE_NAMES, ',').trim());
    for (final String resource : RESOURCE_NAMES) {
      serverConfiguration.addProperty(getKey("pinot.server.instance", resource.trim(), "numQueryExecutorThreads"), "50");
      serverConfiguration.addProperty(getKey("pinot.server.instance", resource.trim(), "dataManagerType"), "offline");
      serverConfiguration.addProperty(getKey("pinot.server.instance", resource.trim(), "readMode"), SERVER_INDEX_READ_MODE);
    }
    serverConfiguration.addProperty("pinot.server.instance.data.manager.class", "com.linkedin.pinot.core.data.manager.InstanceDataManager");
    serverConfiguration.addProperty("pinot.server.instance.segment.metadata.loader.class",
        "com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader");
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.class",
        "TimeSegmentPruner,DataSchemaSegmentPruner,TableNameSegmentPruner");
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.TimeSegmentPruner.id", "0");
    serverConfiguration.addProperty("pinot.server.query.executor.pruner.DataSchemaSegmentPruner.id", "1");
    serverConfiguration
    .addProperty("pinot.server.query.executor.class", "com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl");
    serverConfiguration.addProperty("pinot.server.requestHandlerFactory.class",
        "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory");
    serverConfiguration.addProperty("pinot.server.netty.port", SERVER_PORT);
    serverConfiguration.setDelimiterParsingDisabled(false);
    return serverConfiguration;
  }

  @SuppressWarnings("unchecked")
  private void log(PropertiesConfiguration props, String configsFor) {
    logger.info("******************************* configs for : " + configsFor + " : ********************************************");

    final Iterator<String> keys = props.getKeys();

    while (keys.hasNext()) {
      final String key = keys.next();
      logger.info(key + " : " + props.getProperty(key));
    }

    logger.info("******************************* configs end for : " + configsFor + " : ****************************************");
  }

  private void startServer() {
    try {
      serverInstance.start();
    } catch (final Exception e) {
      logger.error(e);
    }
  }

  private void stopServer() {
    try {
      serverInstance.shutDown();
    } catch (final Exception e) {
      logger.error(e);
    }
  }

  private void startBroker() {
    try {
      bld.start();
    } catch (final Exception e) {
      logger.error(e);
    }
  }

  private void stopBroker() {
    try {
      bld.stop();
    } catch (final Exception e) {
      logger.error(e);
    }
  }


  public void startAll() throws Exception {
    final PropertiesConfiguration broker = brokerProperties();
    final PropertiesConfiguration server = serverProperties();
    log(broker, "broker");
    log(server, "server");

    System.out.println("************************ 1");
    serverInstance = new ServerInstance();
    System.out.println("************************ 2");
    serverInstance.init(new ServerConf(server));
    System.out.println("************************ 3");
    bld = new BrokerServerBuilder(broker, null);
    System.out.println("************************ 4");
    bld.buildNetwork();
    System.out.println("************************ 5");
    bld.buildHTTP();
    System.out.println("************************ 6");

    startServer();
    startBroker();

  }

  public void stopAll() {
    stopServer();
    stopBroker();
  }
}