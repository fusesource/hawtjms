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
package io.hawtjms.sasl;

import io.hawtjms.util.FactoryFinder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MechanismFactoryFinder {

    private static final FactoryFinder MECHANISM_FACTORY_FINDER =
        new FactoryFinder("META-INF/services/io/hawtjms/sasl/");
    private static final ConcurrentHashMap<String, MechanismFactory> MECHANISM_FACTORYS =
        new ConcurrentHashMap<String, MechanismFactory>();

    /**
     * Finds and returns a MechanismFactory for the given URI.  The factory will be returned
     * in a configured state using the URI to set all options.
     *
     * @param name
     *        The name of the authentication mechanism to search for..
     *
     * @return the configured MechanismFactory.
     *
     * @throws Exception if an error occurs while locating and loading the Factory.
     */
    public static MechanismFactory locate(String name) throws Exception {
        MechanismFactory factory = findMechanismFactory(name);
        return factory;
    }

    /**
     * Allow registration of a Mechanism factory without wiring via META-INF classes
     *
     * @param name
     *        The name that identifies the authentication Mechanism.
     * @param factory
     *        The MechanismFactory to register in this finder.
     */
    public static void registerMechanismFactory(String name, MechanismFactory factory) {
        MECHANISM_FACTORYS.put(name, factory);
    }

    /**
     * Searches for a MechanismFactory by using the scheme from the given name.
     *
     * The search first checks the local cache of mechanism factories before moving on
     * to search in the classpath.
     *
     * @param name
     *        The name of the authentication mechanism to search for..
     *
     * @return a mechanism factory instance matching the URI's scheme.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static MechanismFactory findMechanismFactory(String name) throws IOException {
        if (name == null || name.isEmpty()) {
            throw new IOException("No Mechanism name specified: [" + name + "]");
        }

        MechanismFactory factory = MECHANISM_FACTORYS.get(name);
        if (factory == null) {
            try {
                factory = (MechanismFactory) MECHANISM_FACTORY_FINDER.newInstance(name);
                MECHANISM_FACTORYS.put(name, factory);
            } catch (Throwable e) {
                throw new IOException("Mechanism scheme NOT recognized: [" + name + "]", e);
            }
        }

        return factory;
    }
}
