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
package org.fusesource.amqpjms.provider.amqp.sasl;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SaslEngineFactoryImpl implements SaslEngineFactory {

    private static final String PLAIN = "PLAIN";
    private static final String ANONYMOUS = "ANONYMOUS";

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final Collection<String> DEFAULT_MECHS = Arrays.asList(PLAIN, ANONYMOUS);

    private interface PropertyUsingSaslEngine extends SaslEngine {

        void setProperties(Map<String, Object> properties);

        boolean isValid();
    }

    abstract public class AbstractUsernamePasswordEngine implements PropertyUsingSaslEngine {
        private String _username;
        private String _password;

        public String getUsername() {
            return _username;
        }

        public String getPassword() {
            return _password;
        }

        @Override
        public void setProperties(Map<String, Object> properties) {
            Object user = properties.get(USERNAME_PROPERTY);
            if (user instanceof String) {
                _username = (String) user;
            }
            Object pass = properties.get(PASSWORD_PROPERTY);
            if (pass instanceof String) {
                _password = (String) pass;
            }
        }

        @Override
        public boolean isValid() {
            return _username != null && _password != null;
        }
    }

    public class PlainEngine extends AbstractUsernamePasswordEngine {
        private boolean _sentInitialResponse;

        @Override
        public String getMechanism() {
            return PLAIN;
        }

        @Override
        public byte[] getResponse(byte[] challenge) {
            if (!_sentInitialResponse) {
                byte[] usernameBytes = getUsername().getBytes();
                byte[] passwordBytes = getPassword().getBytes();
                byte[] data = new byte[usernameBytes.length + passwordBytes.length + 2];
                System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
                System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);
                _sentInitialResponse = true;
                return data;
            }

            return EMPTY_BYTES;
        }
    }

    public class AnonymousEngine implements PropertyUsingSaslEngine {
        @Override
        public String getMechanism() {
            return ANONYMOUS;
        }

        @Override
        public void setProperties(Map<String, Object> properties) {
        }

        @Override
        public byte[] getResponse(byte[] challenge) {
            return EMPTY_BYTES;
        }

        @Override
        public boolean isValid() {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public SaslEngine createSaslEngine(Map<String, Object> properties, String... mechanisms) {
        List<String> mechanismList = Arrays.asList(mechanisms);

        Collection<String> preferredMechs;
        if (properties.get(PREFERRED_MECHANISMS_PROPERTY) instanceof Collection) {
            preferredMechs = (Collection<String>) properties.get(PREFERRED_MECHANISMS_PROPERTY);
        } else {
            preferredMechs = DEFAULT_MECHS;
        }

        PropertyUsingSaslEngine engine = null;
        for (String mech : preferredMechs) {
            if (mechanismList.contains(mech)) {
                if (PLAIN.equals(mech)) {
                    engine = new PlainEngine();
                } else if (ANONYMOUS.equals(mech)) {
                    engine = new AnonymousEngine();
                }
                if (engine != null) {
                    engine.setProperties(properties);
                    if (engine.isValid()) {
                        break;
                    } else {
                        engine = null;
                    }
                }
            }
        }

        return engine;
    }
}
