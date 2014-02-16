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
package org.fusesource.amqpjms.jms;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.net.ssl.SSLContext;

import org.fusesource.amqpjms.jms.exceptions.JmsExceptionSupport;
import org.fusesource.amqpjms.jms.jndi.JNDIStorable;
import org.fusesource.amqpjms.jms.util.IdGenerator;
import org.fusesource.amqpjms.jms.util.PropertyUtil;
import org.fusesource.amqpjms.provider.Provider;
import org.fusesource.amqpjms.provider.ProviderFactory;
import org.fusesource.amqpjms.provider.ProviderFactoryFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS ConnectionFactory Implementation.
 */
public class JmsConnectionFactory extends JNDIStorable implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionFactory.class);

    private URI brokerURI;
    private URI localURI;
    private String username;
    private String password;
    private SSLContext sslContext;
    private boolean forceAsyncSend;
    private boolean omitHost;
    private String queuePrefix = "/queue/";
    private String topicPrefix = "/topic/";
    private String tempQueuePrefix = "/temp-queue/";
    private String tempTopicPrefix = "/temp-topic/";
    private long disconnectTimeout = 10000;
    private IdGenerator clientIdGenerator;
    private String clientIDPrefix;
    private IdGenerator connectionIdGenerator;
    private String connectionIDPrefix;
    private ExceptionListener exceptionListener;

    private JmsPrefetchPolicy prefetchPolicy = new JmsPrefetchPolicy();

    public JmsConnectionFactory() {
    }

    public JmsConnectionFactory(String username, String password) {
        setUsername(username);
        setPassword(password);
    }

    public JmsConnectionFactory(String brokerURI) {
        this(createURI(brokerURI));
    }

    public JmsConnectionFactory(URI brokerURI) {
        setBrokerURI(brokerURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, URI brokerURI) {
        setUsername(userName);
        setPassword(password);
        setBrokerURI(brokerURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, String brokerURI) {
        setUsername(userName);
        setPassword(password);
        setBrokerURI(brokerURI);
    }

    /**
     * Set properties
     *
     * @param props
     */
    public void setProperties(Properties props) {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        setProperties(map);
    }

    @Override
    public void setProperties(Map<String, String> map) {
        buildFromProperties(map);
    }

    /**
     * @param map
     */
    @Override
    protected void buildFromProperties(Map<String, String> map) {
        PropertyUtil.setProperties(this, map);
    }

    /**
     * @param map
     */
    @Override
    protected void populateProperties(Map<String, String> map) {
        try {
            Map<String, String> result = PropertyUtil.getProperties(this);
            map.putAll(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return a TopicConnection
     * @throws JMSException
     * @see javax.jms.TopicConnectionFactory#createTopicConnection()
     */
    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createTopicConnection(getUsername(), getPassword());
    }

    /**
     * @param userName
     * @param password
     * @return a TopicConnection
     * @throws JMSException
     * @see javax.jms.TopicConnectionFactory#createTopicConnection(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(brokerURI);
            JmsTopicConnection result = new JmsTopicConnection(connectionId, provider, getClientIdGenerator());
            PropertyUtil.setProperties(result, PropertyUtil.getProperties(this));
            return result;
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @return a Connection
     * @throws JMSException
     * @see javax.jms.ConnectionFactory#createConnection()
     */
    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(getUsername(), getPassword());
    }

    /**
     * @param userName
     * @param password
     * @return Connection
     * @throws JMSException
     * @see javax.jms.ConnectionFactory#createConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(brokerURI);
            JmsConnection result = new JmsConnection(connectionId, provider, getClientIdGenerator());
            PropertyUtil.setProperties(result, PropertyUtil.getProperties(this));
            return result;
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @return a QueueConnection
     * @throws JMSException
     * @see javax.jms.QueueConnectionFactory#createQueueConnection()
     */
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createQueueConnection(getUsername(), getPassword());
    }

    /**
     * @param userName
     * @param password
     * @return a QueueConnection
     * @throws JMSException
     * @see javax.jms.QueueConnectionFactory#createQueueConnection(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(brokerURI);
            JmsQueueConnection result = new JmsQueueConnection(connectionId, provider, getClientIdGenerator());
            PropertyUtil.setProperties(result, PropertyUtil.getProperties(this));
            return result;
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    private Provider createProvider(URI brokerURI) throws Exception {
        Provider result = null;

        try {
            ProviderFactory factory = ProviderFactoryFinder.findProviderFactory(brokerURI);
            result = factory.createProvider();
            result.connect();
        } catch (Exception ex) {
            LOG.error("Failed to create JMS Provider instance: ", ex);
            throw ex;
        }

        return result;
    }

    private static URI createURI(String name) {
        if (name != null && name.trim().isEmpty() == false) {
            try {
                return new URI(name);
            } catch (URISyntaxException e) {
                throw (IllegalArgumentException) new IllegalArgumentException("Invalid broker URI: " + name).initCause(e);
            }
        }
        return null;
    }

    protected synchronized IdGenerator getConnectionIdGenerator() {
        if (connectionIdGenerator == null) {
            if (connectionIDPrefix != null) {
                connectionIdGenerator = new IdGenerator(connectionIDPrefix);
            } else {
                connectionIdGenerator = new IdGenerator();
            }
        }
        return connectionIdGenerator;
    }

    protected void setConnectionIdGenerator(IdGenerator connectionIdGenerator) {
        this.connectionIdGenerator = connectionIdGenerator;
    }

    //////////////////////////////////////////////////////////////////////////
    // Property getters and setters
    //////////////////////////////////////////////////////////////////////////

    /**
     * @return the brokerURI
     */
    public String getBrokerURI() {
        return this.brokerURI != null ? this.brokerURI.toString() : "";
    }

    /**
     * @param brokerURI
     *        the brokerURI to set
     */
    public void setBrokerURI(String brokerURI) {
        if (brokerURI == null) {
            throw new IllegalArgumentException("brokerURI cannot be null");
        }
        this.brokerURI = createURI(brokerURI);
    }

    /**
     * @return the localURI
     */
    public String getLocalURI() {
        return this.localURI != null ? this.localURI.toString() : "";
    }

    /**
     * @param localURI
     *        the localURI to set
     */
    public void setLocalURI(String localURI) {
        this.localURI = createURI(localURI);
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @param username
     *        the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @param password
     *        the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isForceAsyncSend() {
        return forceAsyncSend;
    }

    public void setForceAsyncSend(boolean forceAsyncSend) {
        this.forceAsyncSend = forceAsyncSend;
    }

    public boolean isOmitHost() {
        return omitHost;
    }

    public void setOmitHost(boolean omitHost) {
        this.omitHost = omitHost;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public String getTempQueuePrefix() {
        return tempQueuePrefix;
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        this.tempQueuePrefix = tempQueuePrefix;
    }

    public String getTempTopicPrefix() {
        return tempTopicPrefix;
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        this.tempTopicPrefix = tempTopicPrefix;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public long getDisconnectTimeout() {
        return disconnectTimeout;
    }

    public void setDisconnectTimeout(long disconnectTimeout) {
        this.disconnectTimeout = disconnectTimeout;
    }

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(JmsPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public String getClientIDPrefix() {
        return clientIDPrefix;
    }

    /**
     * Sets the prefix used by auto-generated JMS Client ID values which are used if the JMS
     * client does not explicitly specify on.
     *
     * @param clientIDPrefix
     */
    public void setClientIDPrefix(String clientIDPrefix) {
        this.clientIDPrefix = clientIDPrefix;
    }

    protected synchronized IdGenerator getClientIdGenerator() {
        if (clientIdGenerator == null) {
            if (clientIDPrefix != null) {
                clientIdGenerator = new IdGenerator(clientIDPrefix);
            } else {
                clientIdGenerator = new IdGenerator();
            }
        }
        return clientIdGenerator;
    }

    protected void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    /**
     * Sets the prefix used by connection id generator.
     *
     * @param connectionIDPrefix
     *        The string prefix used on all connection Id's created by this factory.
     */
    public void setConnectionIDPrefix(String connectionIDPrefix) {
        this.connectionIDPrefix = connectionIDPrefix;
    }

    /**
     * Gets the currently configured JMS ExceptionListener that will be set on all
     * new Connection objects created from this factory.
     *
     * @return the currently configured JMS ExceptionListener.
     */
    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    /**
     * Sets the JMS ExceptionListener that will be set on all new Connection objects
     * created from this factory.
     *
     * @param exceptionListener
     *        the JMS ExceptionListenenr to apply to new Connection's or null to clear.
     */
    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }
}
