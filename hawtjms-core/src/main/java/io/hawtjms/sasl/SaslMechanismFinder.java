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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to find a SASL Mechanism that most closely matches the preferred set
 * of Mechanisms supported by the remote peer.
 *
 * The Matching mechanism is chosen by first find all instances of SASL
 * mechanism types that are supported on the remote peer, and then making a
 * final selection based on the Mechanism in the found set that has the
 * highest priority value.
 */
public class SaslMechanismFinder {

    private static final Logger LOG = LoggerFactory.getLogger(SaslMechanismFinder.class);

    /**
     * Attempts to find a matching Mechanism implementation given a list of supported
     * mechanisms from a remote peer.  Can return null if no matching Mechanisms are
     * found.
     *
     * @param remoteMechanisms
     *        list of mechanism names that are supported by the remote peer.
     *
     * @return the best matching Mechanism for the supported remote set.
     */
    public static Mechanism findMatchingMechanism(String...remoteMechanisms) {

        Mechanism match = null;
        List<Mechanism> found = new ArrayList<Mechanism>();

        for (String remoteMechanism : remoteMechanisms) {
            try {
                MechanismFactory factory = MechanismFactoryFinder.findMechanismFactory(remoteMechanism);
                found.add(factory.createMechanism());
            } catch (IOException e) {
                LOG.warn("Caught exception while searching for SASL mechanisms: {}", e.getMessage());
            }
        }

        if (!found.isEmpty()) {
            // Sorts by priority using Mechanism comparison and return the last value in
            // list which is the Mechanism deemed to be the highest priority match.
            Collections.sort(found);
            match = found.get(found.size() - 1);
        }

        LOG.info("Best match for SASL auth was: {}", match);

        return match;
    }
}
