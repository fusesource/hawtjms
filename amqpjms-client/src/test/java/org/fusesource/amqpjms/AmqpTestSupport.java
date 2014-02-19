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
package org.fusesource.amqpjms;

import java.util.Set;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpTestSupport.class);
    protected BrokerService brokerService;
    protected Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    protected int port;

    public static void main(String[] args) throws Exception {
        final AmqpTestSupport s = new AmqpTestSupport();
        s.port = 5672;
        s.startBroker();
        while (true) {
            Thread.sleep(100000);
        }
    }

    @Before
    public void setUp() throws Exception {
        exceptions.clear();
        startBroker();
        this.numberOfMessages = 2000;
    }

    protected void createBroker(boolean deleteAllMessages) throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setUseJmx(true);

        addAMQPConnector();
    }

    protected void addAMQPConnector() throws Exception {
        TransportConnector connector = brokerService.addConnector("amqp://0.0.0.0:" + port);
        port = connector.getConnectUri().getPort();
        LOG.debug("Using amqp port " + port);
    }

    public void startBroker() throws Exception {
        if (brokerService != null) {
            throw new IllegalStateException("Broker is already created.");
        }

        createBroker(true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void restartBroker() throws Exception {
        stopBroker();
        createBroker(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
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
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost,connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);

        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}