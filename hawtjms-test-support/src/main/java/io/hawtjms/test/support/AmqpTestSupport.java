/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hawtjms.test.support;

import io.hawtjms.jms.JmsConnectionFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.JMXSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport {

    public static final String KAHADB_DIRECTORY = "target/activemq-data";

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    protected BrokerService brokerService;
    protected final List<BrokerService> brokers = new ArrayList<BrokerService>();
    protected final Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    protected int amqpPort;
    protected int openwirePort;
    protected Connection connection;

    @Before
    public void setUp() throws Exception {
        exceptions.clear();
        startPrimaryBroker();
        this.numberOfMessages = 2000;
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        stopPrimaryBroker();
        for (BrokerService broker : brokers) {
            try {
                stopBroker(broker);
            } catch (Exception ex) {}
        }
    }

    protected boolean isPersistent() {
        return false;
    }

    protected boolean isAdvisorySupport() {
        return false;
    }

    protected boolean isAmqpDiscovery() {
        return false;
    }

    protected BrokerService createBroker(String name, boolean deleteAllMessages, int amqpPort, int openwirePort) throws Exception {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File(KAHADB_DIRECTORY + "/" + name));

        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName(name);
        brokerService.setPersistent(isPersistent());
        brokerService.setAdvisorySupport(isAdvisorySupport());
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setUseJmx(true);
        brokerService.setDataDirectory("target/" + name);
        brokerService.setPersistenceAdapter(kaha);
        brokerService.setStoreOpenWireVersion(10);

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();
        BrokerPlugin authenticationPlugin = configureAuthentication();
        if (authenticationPlugin != null) {
            plugins.add(configureAuthorization());
        }

        BrokerPlugin authorizationPlugin = configureAuthorization();
        if (authorizationPlugin != null) {
            plugins.add(configureAuthentication());
        }

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

        addAMQPConnector(brokerService, amqpPort);
        addOpenWireConnector(brokerService, openwirePort);

        return brokerService;
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        users.add(new AuthenticationUser("user", "password", "users"));
        users.add(new AuthenticationUser("guest", "password", "guests"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        return authenticationPlugin;
    }

    protected void addAMQPConnector(BrokerService brokerService, int port) throws Exception {
        TransportConnector connector = brokerService.addConnector("amqp://0.0.0.0:" + port);
        connector.setName("amqp");
        if (isAmqpDiscovery()) {
            connector.setDiscoveryUri(new URI("multicast://default"));
        }
        amqpPort = connector.getConnectUri().getPort();
        LOG.debug("Using amqp port: {}", port);
    }

    protected void addOpenWireConnector(BrokerService brokerService, int port) throws Exception {
        TransportConnector connector = brokerService.addConnector("tcp://0.0.0.0:" + port);
        connector.setName("openwire");
        openwirePort = connector.getConnectUri().getPort();
        LOG.debug("Using openwire port: {}", port);
    }

    public void startPrimaryBroker() throws Exception {
        if (brokerService != null && brokerService.isStarted()) {
            throw new IllegalStateException("Broker is already created.");
        }

        brokerService = createBroker("localhost", true, amqpPort, openwirePort);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void restartPrimaryBroker() throws Exception {
        stopBroker(brokerService);
        brokerService = restartBroker(brokerService);
    }

    public void stopPrimaryBroker() throws Exception {
        stopBroker(brokerService);
    }

    public void startNewBroker() throws Exception {
        int offset = brokers.size() + 1;
        String brokerName = "localhost" + offset;
        int amqpPort = this.amqpPort + offset;
        int openwirePort = this.openwirePort + offset;

        BrokerService brokerService = createBroker(brokerName, true, amqpPort, openwirePort);
        brokerService.setUseJmx(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        brokers.add(brokerService);
    }

    public BrokerService restartBroker(BrokerService brokerService) throws Exception {
        String name = brokerService.getBrokerName();
        int amqpPort = brokerService.getTransportConnectorByName("amqp").getConnectUri().getPort();
        int openwirePort = brokerService.getTransportConnectorByName("openwire").getConnectUri().getPort();

        stopBroker(brokerService);
        BrokerService broker = createBroker(name, false, amqpPort, openwirePort);
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    public void stopBroker(BrokerService broker) throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public URI getBrokerAmqpConnectionURI() {
        try {
            return new URI("amqp://127.0.0.1:" + amqpPort);
        } catch (URISyntaxException e) {
            throw new RuntimeException();
        }
    }

    public URI getBrokerOpenWireConnectionURI() {
        try {
            return new URI("tcp://127.0.0.1:" + openwirePort);
        } catch (URISyntaxException e) {
            throw new RuntimeException();
        }
    }

    public String getAmqpFailoverURI() throws Exception {
        StringBuilder uri = new StringBuilder();
        uri.append("failover://(");
        uri.append(brokerService.getTransportConnectorByName("amqp").getPublishableConnectString());

        for (BrokerService broker : brokers) {
            uri.append(",");
            uri.append(broker.getTransportConnectorByName("amqp").getPublishableConnectString());
        }

        uri.append(")");

        return uri.toString();
    }

    public List<URI> getBrokerURIs() throws Exception {
        ArrayList<URI> result = new ArrayList<URI>();
        result.add(brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI());

        for (BrokerService broker : brokers) {
            result.add(broker.getTransportConnectorByName("amqp").getPublishableConnectURI());
        }

        return result;
    }

    public Connection createAmqpConnection() throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI());
    }

    public Connection createAmqpConnection(String username, String password) throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
    }

    public Connection createAmqpConnection(URI brokerURI) throws Exception {
        return createAmqpConnection(brokerURI, null, null);
    }

    public Connection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        if (username != null) {
            factory.setUsername(username);
        }
        if (password != null) {
            factory.setPassword(password);
        }
        return factory.createConnection();
    }

    public Connection createActiveMQConnection() throws Exception {
        return createActiveMQConnection(getBrokerOpenWireConnectionURI());
    }

    public Connection createActiveMQConnection(URI brokerURI) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
        return factory.createConnection();
    }

    public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = session.createProducer(destination);

        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("TextMessage: " + i);
            p.send(message);
        }

        session.close();
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        return getProxyToBroker(brokerService);
    }

    protected BrokerViewMBean getProxyToBroker(BrokerService broker) throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = broker.getBrokerObjectName();
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        return getProxyToConnectionView(connectionType);
    }

    protected ConnectorViewMBean getProxyToConnectionView(BrokerService broker, String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            broker.getBrokerObjectName() + ",connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);
        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToQueue(brokerService, name);
    }

    protected QueueViewMBean getProxyToQueue(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=Queue,destinationName=" + name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToTemporaryQueue(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTemporaryQueue(brokerService, name);
    }

    protected QueueViewMBean getProxyToTemporaryQueue(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        name = JMXSupport.encodeObjectNamePart(name);
        ObjectName queueViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=TempQueue,destinationName=" + name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTopic(brokerService, name);
    }

    protected TopicViewMBean getProxyToTopic(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        ObjectName topicViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=Topic,destinationName=" + name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTemporaryTopic(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTemporaryTopic(brokerService, name);
    }

    protected TopicViewMBean getProxyToTemporaryTopic(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        name = JMXSupport.encodeObjectNamePart(name);
        ObjectName topicViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=TempTopic,destinationName=" + name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
            .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }

    protected void sendToAmqQueue(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue amqTestQueue = amqSession.createQueue(name.getMethodName());
        sendMessages(activemqConnection, amqTestQueue, count);
    }

    protected void sendToAmqTopic(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic amqTestTopic = amqSession.createTopic(name.getMethodName());
        sendMessages(activemqConnection, amqTestTopic, count);
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users,anonymous");
        entry.setWrite("users,anonymous");
        entry.setAdmin("users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users,anonymous");
        entry.setWrite("users,anonymous");
        entry.setAdmin("users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins,anonymous");
        tempEntry.setWrite("admins,anonymous");
        tempEntry.setAdmin("admins,anonymous");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }
}