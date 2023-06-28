/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.routes;

import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.Metadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasRequest;
import org.apache.cassandra.sidecar.common.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;

/**
 * Handler which provides token range to read and write replica mapping
 */
@Singleton
public class TokenRangeReplicaMapHandler extends AbstractHandler<TokenRangeReplicasRequest>
{

    @Inject
    public TokenRangeReplicaMapHandler(InstanceMetadataFetcher metadataFetcher,
                                       CassandraInputValidator validator,
                                       ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               TokenRangeReplicasRequest request)
    {
        String keyspace = request.keyspace();

        logger.debug("TokenRangeReplicaMapHandler received request={}, remoteAddress={}, instance={}",
                     request, remoteAddress, host);

        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);

        StorageOperations storageOperations = delegate.storageOperations();
        Metadata metadata = delegate.metadata();
        if (storageOperations == null || metadata == null)
        {
            context.fail(cassandraServiceUnavailable());
            return;
        }

        executorPools.service().executeBlocking(promise -> {
            try
            {
                context.json(storageOperations.tokenRangeReplicas(keyspace, metadata.getPartitioner()));
            }
            catch (UnknownHostException e)
            {
                processFailure(e, context, request, remoteAddress, host);
            }
        }).onFailure(cause -> processFailure(cause, context, request, remoteAddress, host));
    }

    @Override
    protected TokenRangeReplicasRequest extractParamsOrThrow(RoutingContext context)
    {
        return new TokenRangeReplicasRequest(keyspace(context, true));
    }

    private void processFailure(Throwable cause, RoutingContext context, TokenRangeReplicasRequest request,
                                SocketAddress remoteAddress, String host)
    {
        context.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        if (cause instanceof AssertionError &&
            StringUtils.contains(cause.getMessage(), "Unknown keyspace"))
        {
            context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), cause.getMessage()));
            return;
        }

        super.processFailure(cause, context, host, remoteAddress, request);
    }
}
