package org.apache.cassandra.sidecar.client.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;

/**
 * Represents a request to retrieve information from the token-range replicas endpoint
 */
public class TokenRangeReplicasRequest extends DecodableRequest<TokenRangeReplicasResponse>
{
    /**
     * Constructs a new request to retrieve information by keyspace from token-range replicas endpoint
     */
    public TokenRangeReplicasRequest(String keyspace)
    {
        super(ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace));
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }
}
