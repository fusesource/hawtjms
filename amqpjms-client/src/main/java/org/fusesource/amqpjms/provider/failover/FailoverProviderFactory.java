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
package org.fusesource.amqpjms.provider.failover;

import java.net.URI;
import java.util.Map;

import org.fusesource.amqpjms.provider.AsyncProvider;
import org.fusesource.amqpjms.provider.BlockingProvider;
import org.fusesource.amqpjms.provider.DefaultBlockingProvider;
import org.fusesource.amqpjms.provider.ProviderFactory;
import org.fusesource.amqpjms.util.PropertyUtil;
import org.fusesource.amqpjms.util.URISupport;
import org.fusesource.amqpjms.util.URISupport.CompositeData;

/**
 * Factory for creating instances of the Failover Provider type.
 */
public class FailoverProviderFactory extends ProviderFactory {

    @Override
    public BlockingProvider createProvider(URI remoteURI) throws Exception {

        CompositeData composite = URISupport.parseComposite(remoteURI);
        Map<String, String> options = composite.getParameters();
        Map<String, String> nested = PropertyUtil.filterProperties(options, "nested.");

        FailoverProvider failover = new FailoverProvider(composite.getComponents(), nested);
        if (!PropertyUtil.setProperties(failover, options)) {
            String msg = ""
                + " Not all options could be set on the Failover provider."
                + " Check the options are spelled correctly."
                + " Given parameters=[" + options + "]."
                + " This Provider cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        BlockingProvider provider = new DefaultBlockingProvider(failover);

        return provider;
    }

    @Override
    public AsyncProvider createAsyncProvider(URI remoteURO) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return "Failover";
    }
}
