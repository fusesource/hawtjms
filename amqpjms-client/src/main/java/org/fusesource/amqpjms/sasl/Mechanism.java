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
package org.fusesource.amqpjms.sasl;

/**
 * Interface for all SASL authentication mechanism implementations.
 */
public interface Mechanism extends Comparable<Mechanism> {

    /**
     * Relative priority values used to arrange the found SASL
     * mechanisms in a preferred order where the level of security
     * generally defines the preference.
     */
    public enum PRIORITY {
        LOWEST(0),
        LOW(1),
        MEDIUM(2),
        HIGH(3),
        HIGHEST(4);

        private final int value;

        private PRIORITY(int value) {
            this.value = value;
        }

        public int getValue(){
            return value;
       }
    };

    /**
     * @return return the relative priority of this SASL mechanism.
     */
    int getPriority();

    /**
     * @return the well known name of this SASL mechanism.
     */
    String getName();

    /**
     * @return the response buffer used to answer the SASL challenge.
     */
    byte[] getResponse();
}
