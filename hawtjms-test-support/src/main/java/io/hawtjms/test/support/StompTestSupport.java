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

import java.net.URI;
import java.util.Map;

import javax.jms.Connection;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Support class for STOMP based tests.
 */
public class StompTestSupport extends HawtJmsTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(StompTestSupport.class);

    protected boolean isStompDiscovery() {
        return false;
    }

    @Override
    protected void addAdditionalConnectors(BrokerService brokerService, Map<String, Integer> portMap) throws Exception {
        int port = 0;
        if (portMap.containsKey("stomp")) {
            port = portMap.get("stomp");
        }
        TransportConnector connector = brokerService.addConnector("stomp://0.0.0.0:" + port);
        connector.setName("stomp");
        if (isStompDiscovery()) {
            connector.setDiscoveryUri(new URI("multicast://default"));
        }
        port = connector.getPublishableConnectURI().getPort();
        LOG.debug("Using stomp port: {}", port);
    }

    public URI getBrokerStompConnectionURI() {
        try {
            return new URI("stomp://127.0.0.1:" +
                brokerService.getTransportConnectorByName("stomp").getPublishableConnectURI().getPort());
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public String getStompFailoverURI() throws Exception {
        StringBuilder uri = new StringBuilder();
        uri.append("failover://(");
        uri.append(brokerService.getTransportConnectorByName("stomp").getPublishableConnectString());

        for (BrokerService broker : brokers) {
            uri.append(",");
            uri.append(broker.getTransportConnectorByName("stomp").getPublishableConnectString());
        }

        uri.append(")");

        return uri.toString();
    }

    public Connection createStompConnection() throws Exception {
        return createStompConnection(getBrokerStompConnectionURI());
    }

    public Connection createStompConnection(String username, String password) throws Exception {
        return createStompConnection(getBrokerStompConnectionURI(), username, password);
    }

    public Connection createStompConnection(URI brokerURI) throws Exception {
        return createStompConnection(brokerURI, null, null);
    }

    public Connection createStompConnection(URI brokerURI, String username, String password) throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
        if (username != null) {
            factory.setUsername(username);
        }
        if (password != null) {
            factory.setPassword(password);
        }
        return factory.createConnection();
    }
}
