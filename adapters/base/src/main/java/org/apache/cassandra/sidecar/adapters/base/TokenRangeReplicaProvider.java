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

package org.apache.cassandra.sidecar.adapters.base;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.utils.GossipInfoParser;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.adapters.base.ClusterMembershipJmxOperations.FAILURE_DETECTOR_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;

/**
 * Aggregates the replica-set by token range
 */
public class TokenRangeReplicaProvider
{
    private final JmxClient jmxClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicaProvider.class);

    public TokenRangeReplicaProvider(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    public TokenRangeReplicasResponse tokenRangeReplicas(String keyspace, Partitioner partitioner)
    {
        Objects.requireNonNull(keyspace, "keyspace must be non-null");

        StorageJmxOperations storage = jmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);

        // Retrieve map of primary token ranges to endpoints that describe the ring topology
        Map<List<String>, List<String>> rangeToEndpointMappings = storage.getRangeToEndpointWithPortMap(keyspace);
        // Pending ranges include bootstrap tokens and leaving endpoints as represented in the Cassandra TokenMetadata
        Map<List<String>, List<String>> pendingRangeMappings = storage.getPendingRangeToEndpointWithPortMap(keyspace);

        Set<String> replicaSet = Stream.concat(rangeToEndpointMappings.values().stream().flatMap(List::stream),
                                               pendingRangeMappings.values().stream().flatMap(List::stream))
                                       .collect(Collectors.toSet());

        Map<String, String> hostToDatacenter = groupHostsByDatacenter(replicaSet);

        // Retrieve map of all token ranges (pending & primary) to endpoints
        List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas =
        writeReplicasFromPendingRanges(rangeToEndpointMappings,
                                       pendingRangeMappings,
                                       hostToDatacenter,
                                       partitioner,
                                       keyspace);

        Map<String, String> replicaToStateMap = replicaToStateMap(replicaSet, storage);

        return new TokenRangeReplicasResponse(
        replicaToStateMap,
        writeReplicas,
        mappingsToUnwrappedReplicaSet(rangeToEndpointMappings, hostToDatacenter, partitioner));
    }

    private Map<String, String> replicaToStateMap(Set<String> replicaSet, StorageJmxOperations storage)
    {
        List<String> joiningNodes = storage.getJoiningNodesWithPort();
        List<String> leavingNodes = storage.getLeavingNodesWithPort();
        List<String> movingNodes = storage.getMovingNodesWithPort();

        String rawGossipInfo = getRawGossipInfo();
        GossipInfoResponse gossipInfo = GossipInfoParser.parse(rawGossipInfo);

        StateWithReplacement state = new StateWithReplacement(joiningNodes, leavingNodes, movingNodes, gossipInfo);

        return replicaSet.stream()
                         .collect(Collectors.toMap(Function.identity(), state::of));
    }

    private String getRawGossipInfo()
    {
        return jmxClient.proxy(ClusterMembershipJmxOperations.class, FAILURE_DETECTOR_OBJ_NAME)
                        .getAllEndpointStatesWithPort();
    }

    private List<TokenRangeReplicasResponse.ReplicaInfo>
    writeReplicasFromPendingRanges(Map<List<String>, List<String>> naturalReplicaMappings,
                                   Map<List<String>, List<String>> pendingRangeMappings,
                                   Map<String, String> hostToDatacenter,
                                   Partitioner partitioner,
                                   String keyspace)
    {
        LOGGER.debug("Pending token ranges for keyspace={}, pendingRangeMappings={}", keyspace, pendingRangeMappings);
        // Merge natural and pending range replicas to generate candidates for write-replicas
        List<TokenRangeReplicas> replicas = Stream.concat(
                                                  naturalReplicaMappings.entrySet().stream(),
                                                  pendingRangeMappings.entrySet().stream())
                                                  .map(entry -> TokenRangeReplicas.generateTokenRangeReplicas(
                                                  new BigInteger(entry.getKey().get(0)),
                                                  new BigInteger(entry.getKey().get(1)),
                                                  partitioner,
                                                  new HashSet<>(entry.getValue())))
                                                  .flatMap(Collection::stream)
                                                  .collect(Collectors.toList());

        // Candidate write-replica mappings (merged from natural and pending ranges) are normalized
        // by consolidating overlapping ranges
        return TokenRangeReplicas.normalize(replicas).stream()
                                 .map(range -> {
                                     Map<String, List<String>> replicasByDc =
                                     replicasByDataCenter(hostToDatacenter, range.replicaSet());
                                     return new TokenRangeReplicasResponse.ReplicaInfo(range.start().toString(),
                                                                                       range.end().toString(),
                                                                                       replicasByDc);
                                 })
                                 .collect(Collectors.toList());
    }

    private List<TokenRangeReplicasResponse.ReplicaInfo>
    mappingsToUnwrappedReplicaSet(Map<List<String>, List<String>> replicasByTokenRange,
                                  Map<String, String> hostToDatacenter,
                                  Partitioner partitioner)
    {
        return replicasByTokenRange.entrySet().stream()
                                   .map(entry -> TokenRangeReplicas.generateTokenRangeReplicas(
                                   new BigInteger(entry.getKey().get(0)),
                                   new BigInteger(entry.getKey().get(1)),
                                   partitioner,
                                   new HashSet<>(entry.getValue())))
                                   .flatMap(Collection::stream)
                                   .sorted()
                                   .map(rep -> {
                                       Map<String, List<String>> replicasByDc =
                                       replicasByDataCenter(hostToDatacenter, rep.replicaSet());
                                       return new TokenRangeReplicasResponse.ReplicaInfo(rep.start().toString(),
                                                                                         rep.end().toString(),
                                                                                         replicasByDc);
                                   })
                                   .collect(Collectors.toList());
    }

    private Map<String, String> groupHostsByDatacenter(Set<String> replicaSet)
    {
        EndpointSnitchJmxOperations endpointSnitchInfo = jmxClient.proxy(EndpointSnitchJmxOperations.class,
                                                                         ENDPOINT_SNITCH_INFO_OBJ_NAME);

        return replicaSet.stream()
                         .collect(Collectors.toMap(Function.identity(),
                                                   (String host) -> getDatacenter(endpointSnitchInfo, host)));
    }

    private String getDatacenter(EndpointSnitchJmxOperations endpointSnitchInfo, String host)
    {
        try
        {
            return endpointSnitchInfo.getDatacenter(host);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static Map<String, List<String>> replicasByDataCenter(Map<String, String> hostToDatacenter,
                                                                  Collection<String> replicas)
    {
        return replicas.stream().collect(Collectors.groupingBy(hostToDatacenter::get));
    }

    /**
     * We want to identity a joining node, to replace a dead node, differently from a newly joining node. To
     * do this we analyze gossip info and set 'Replacing' state for node replacing a dead node.
     * {@link StateWithReplacement} is used to set replacing state for a node.
     *
     * <p>We are adding this state for token range replica provider endpoint. To send out replicas for a
     * range along with state of replicas including replacing state.
     */
    static class StateWithReplacement extends RingProvider.State
    {
        private static final String STATE_REPLACING = "Replacing";
        private final Set<String> joiningNodes;
        private final GossipInfoResponse gossipInfo;

        StateWithReplacement(List<String> joiningNodes, List<String> leavingNodes, List<String> movingNodes,
                             GossipInfoResponse gossipInfo)
        {
            super(joiningNodes, leavingNodes, movingNodes);
            this.joiningNodes = new HashSet<>(joiningNodes);
            this.gossipInfo = gossipInfo;
        }

        /**
         * This method returns state of a node and accounts for a new 'Replacing' state if the node is
         * replacing a dead node. For returning this state, the method checks status of the node in gossip
         * information.
         *
         * @param endpoint node information represented usually in form of 'ip:port'
         * @return Node status
         */
        @Override
        String of(String endpoint)
        {
            if (joiningNodes.contains(endpoint))
            {
                GossipInfoResponse.GossipInfo gossipInfoEntry = gossipInfo.get(endpoint);

                if (gossipInfoEntry != null)
                {
                    LOGGER.info("Found gossipInfoEntry={}", gossipInfoEntry);
                    String hostStatus = gossipInfoEntry.status();
                    if (hostStatus != null && hostStatus.startsWith("BOOT_REPLACE,"))
                    {
                        return STATE_REPLACING;
                    }
                }
            }
            return super.of(endpoint);
        }
    }
}
