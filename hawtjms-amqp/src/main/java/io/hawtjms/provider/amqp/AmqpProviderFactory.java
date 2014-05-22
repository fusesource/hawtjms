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
package io.hawtjms.provider.amqp;

import io.hawtjms.provider.AsyncProvider;
import io.hawtjms.provider.BlockingProvider;
import io.hawtjms.provider.DefaultBlockingProvider;
import io.hawtjms.provider.ProviderFactory;
import io.hawtjms.util.PropertyUtil;

import java.net.URI;
import java.util.Map;

/**
 * Factory for creating the AMQP provider.
 */
public class AmqpProviderFactory extends ProviderFactory {

    @Override
    public BlockingProvider createProvider(URI remoteURI) throws Exception {
        return new DefaultBlockingProvider(createAsyncProvider(remoteURI));
    }

    @Override
    public AsyncProvider createAsyncProvider(URI remoteURI) throws Exception {

        Map<String, String> map = PropertyUtil.parseQuery(remoteURI.getQuery());
        Map<String, String> providerOptions = PropertyUtil.filterProperties(map, "provider.");

        remoteURI = PropertyUtil.replaceQuery(remoteURI, map);

        AsyncProvider result = new AmqpProvider(remoteURI);

        if (!PropertyUtil.setProperties(result, providerOptions)) {
            String msg = ""
                + " Not all provider options could be set on the AMQP Provider."
                + " Check the options are spelled correctly."
                + " Given parameters=[" + providerOptions + "]."
                + " This provider instance cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        return result;
    }

    @Override
    public String getName() {
        return "AMQP";
    }
}
