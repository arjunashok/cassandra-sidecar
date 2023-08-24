///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.sidecar.routes;
//
//import java.math.BigInteger;
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.function.BiConsumer;
//import java.util.stream.Collectors;
//
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.Range;
//import com.google.common.collect.Sets;
//import com.google.common.util.concurrent.Uninterruptibles;
//
//import org.junit.jupiter.api.extension.ExtendWith;
//
//import io.netty.handler.codec.http.HttpResponseStatus;
//import io.vertx.junit5.VertxExtension;
//import io.vertx.junit5.VertxTestContext;
//import org.apache.cassandra.config.CassandraRelevantProperties;
//import org.apache.cassandra.distributed.UpgradeableCluster;
//import org.apache.cassandra.distributed.api.Feature;
//import org.apache.cassandra.distributed.api.IUpgradeableInstance;
//import org.apache.cassandra.distributed.api.TokenSupplier;
//import org.apache.cassandra.distributed.shared.ClusterUtils;
//import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
//import org.apache.cassandra.testing.CassandraIntegrationTest;
//import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
//
//import static org.apache.cassandra.sidecar.routes.BaseTokenRangeIntegrationTest.BBHelperReplacementsMultiDC.NODE_START;
//import static org.assertj.core.api.Assertions.assertThat;
//
///**
// * Node replacement scenarios integration tests for token range replica mapping endpoint with cassandra container.
// * Node replacement tests are temporarily disabled as they depend on a fix for CASSANDRA-18583
// *
// */
//@ExtendWith(VertxExtension.class)
//public class TokenRangeIntegrationReplacementTest extends BaseTokenRangeIntegrationTest
//{
//    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
//    void retrieveMappingWithNodeReplacement(VertxTestContext context,
//                                            ConfigurableCassandraTestContext cassandraTestContext) throws Exception
//    {
//        runReplacementTestScenario(context,
//                                   cassandraTestContext,
//                                   BBHelperReplacementsNode::install,
//                                   BBHelperReplacementsNode.TRANSIENT_STATE_START,
//                                   BBHelperReplacementsNode.TRANSIENT_STATE_END,
//                                   generateExpectedRangeMappingNodeReplacement());
//    }
//
//    @CassandraIntegrationTest(
//    nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
//    void retrieveMappingWithNodeReplacementMultiDC(VertxTestContext context,
//                                                   ConfigurableCassandraTestContext cassandraTestContext)
//    throws Exception
//    {
//
//        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
//        int numNodes = annotation.nodesPerDc() + annotation.newNodesPerDc();
//        UpgradeableCluster cluster = getMultiDCCluster(numNodes,
//                                                       annotation.numDcs(),
//                                                       BBHelperReplacementsMultiDC::install,
//                                                       cassandraTestContext);
//
//        List<IUpgradeableInstance> nodesToRemove = Arrays.asList(cluster.get(3), cluster.get(cluster.size()));
//        runReplacementTestScenario(context,
//                                   cassandraTestContext,
//                                   BBHelperReplacementsMultiDC.TRANSIENT_STATE_START,
//                                   BBHelperReplacementsMultiDC.TRANSIENT_STATE_END,
//                                   cluster,
//                                   nodesToRemove,
//                                   generateExpectedRangeMappingReplacementMultiDC());
//    }
//
//    private void runReplacementTestScenario(VertxTestContext context,
//                                            ConfigurableCassandraTestContext cassandraTestContext,
//                                            BiConsumer<ClassLoader, Integer> instanceInitializer,
//                                            CountDownLatch transientStateStart,
//                                            CountDownLatch transientStateEnd,
//                                            Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
//    throws Exception
//    {
//        UpgradeableCluster cluster =
//        cassandraTestContext.configureAndStartCluster(builder ->
//                                                      builder.withInstanceInitializer(instanceInitializer));
//
//        List<IUpgradeableInstance> nodesToRemove = Arrays.asList(cluster.get(cluster.size()));
//        runReplacementTestScenario(context,
//                                   cassandraTestContext,
//                                   transientStateStart,
//                                   transientStateEnd,
//                                   cluster,
//                                   nodesToRemove,
//                                   expectedRangeMappings);
//    }
//
//    private void runReplacementTestScenario(VertxTestContext context,
//                                            ConfigurableCassandraTestContext cassandraTestContext,
//                                            CountDownLatch transientStateStart,
//                                            CountDownLatch transientStateEnd,
//                                            UpgradeableCluster cluster,
//                                            List<IUpgradeableInstance> nodesToRemove,
//                                            Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
//    throws Exception
//    {
//        try
//        {
//            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
//            Set<String> dcReplication;
//            if (annotation.numDcs() > 1)
//            {
//                createTestKeyspace(ImmutableMap.of("replication_factor", DEFAULT_RF));
//                dcReplication = Sets.newHashSet(Arrays.asList("datacenter1", "datacenter2"));
//            }
//            else
//            {
//                createTestKeyspace(ImmutableMap.of("datacenter1", DEFAULT_RF));
//                dcReplication = Collections.singleton("datacenter1");
//            }
//
//            IUpgradeableInstance seed = cluster.get(1);
//            List<String> removedNodeAddresses = nodesToRemove.stream()
//                                                             .map(n ->
//                                                                  n.config()
//                                                                   .broadcastAddress()
//                                                                   .getAddress()
//                                                                   .getHostAddress())
//                                                             .collect(Collectors.toList());
//
//            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
//            List<String> removedNodeTokens = ring.stream()
//                                                 .filter(i -> removedNodeAddresses.contains(i.getAddress()))
//                                                 .map(ClusterUtils.RingInstanceDetails::getToken)
//                                                 .collect(Collectors.toList());
//
//            stopNodes(seed, nodesToRemove);
//            List<IUpgradeableInstance> newNodes = startReplacementNodes(cluster, nodesToRemove);
//
//            // Wait until replacement nodes are in JOINING state
//            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);
//
//            // Verify state of replacement nodes
//            for (IUpgradeableInstance newInstance : newNodes)
//            {
//                ClusterUtils.awaitRingState(newInstance, newInstance, "Joining");
//                ClusterUtils.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");
//
//                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
//                Optional<ClusterUtils.RingInstanceDetails> replacementInstance = ClusterUtils.ring(seed)
//                                                                                             .stream()
//                                                                                             .filter(
//                                                                                             i -> i.getAddress()
//                                                                                                .equals(newAddress))
//                                                                                             .findFirst();
//                assertThat(replacementInstance).isPresent();
//                // Verify that replacement node tokens match the removed nodes
//                assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
//            }
//
//            retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
//                assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
//                TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
//                assertMappingResponseOK(mappingResponse,
//                                        DEFAULT_RF,
//                                        dcReplication);
//
//                int finalNodeCount = annotation.nodesPerDc() * annotation.numDcs();
//                TokenSupplier tokenSupplier = (annotation.numDcs() > 1) ?
//                                              MultiDcTokenSupplier.evenlyDistributedTokens(
//                                              annotation.nodesPerDc() + annotation.newNodesPerDc(),
//                                              annotation.numDcs(),
//                                              1) :
//                                              TokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc() +
//                                                                                    annotation.newNodesPerDc(),
//                                                                                    1);
//                List<Integer> nodeNums = newNodes.stream().map(i -> i.config().num()).collect(Collectors.toList());
//                validateNodeStates(mappingResponse,
//                                   dcReplication,
//                                   nodeNumber -> nodeNums.contains(nodeNumber) ? "Joining" : "Normal");
//
//                int nodeCount = annotation.nodesPerDc()  * annotation.numDcs();
//                validateTokenRanges(mappingResponse, generateExpectedRanges(nodeCount));
//
//                validateReplicaMapping(mappingResponse, newNodes, expectedRangeMappings);
//                context.completeNow();
//            });
//        }
//        finally
//        {
//            for (int i = 0;
//               i < (cassandraTestContext.annotation.newNodesPerDc() * cassandraTestContext.annotation.numDcs()); i++)
//            {
//                transientStateEnd.countDown();
//            }
//        }
//    }
//
//    private List<IUpgradeableInstance> startReplacementNodes(UpgradeableCluster cluster,
//                                                             List<IUpgradeableInstance> nodesToRemove)
//    {
//        List<IUpgradeableInstance> newNodes = new ArrayList<>();
//        // Launch replacements nodes with the config of the removed nodes
//        for (IUpgradeableInstance removed : nodesToRemove)
//        {
//            // Add new instance for each removed instance as a replacement of its owned token
//            String remAddress = removed.config().broadcastAddress().getAddress().getHostAddress();
//            IUpgradeableInstance replacement = ClusterUtils.addInstance(cluster, removed.config(),
//                                                                        c -> {
//                                                                            c.set("auto_bootstrap", true);
//                                                                            c.with(Feature.GOSSIP,
//                                                                                   Feature.JMX,
//                                                                                   Feature.NATIVE_PROTOCOL);
//                                                                        });
//
//            new Thread(() -> ClusterUtils.start(replacement, (properties) -> {
//                properties.set(CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
//                properties.set(CassandraRelevantProperties.BROADCAST_INTERVAL_MS,
//                               Long.toString(TimeUnit.SECONDS.toMillis(30L)));
//                properties.set(CassandraRelevantProperties.RING_DELAY,
//                               Long.toString(TimeUnit.SECONDS.toMillis(10L)));
//                properties.set(CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS,
//                               TimeUnit.SECONDS.toMillis(10L));
//                properties.set(CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT, remAddress);
//            })).start();
//
//            Uninterruptibles.awaitUninterruptibly(NODE_START, 2, TimeUnit.MINUTES);
//            newNodes.add(replacement);
//        }
//        return newNodes;
//    }
//
//    private void stopNodes(IUpgradeableInstance seed, List<IUpgradeableInstance> removedNodes)
//    {
//        for (IUpgradeableInstance nodeToRemove : removedNodes)
//        {
//            ClusterUtils.stopUnchecked(nodeToRemove);
//            String remAddress = nodeToRemove.config().broadcastAddress().getAddress().getHostAddress();
//
//            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
//            List<ClusterUtils.RingInstanceDetails> match = ring.stream()
//                                                               .filter((d) -> d.getAddress().equals(remAddress))
//                                                               .collect(Collectors.toList());
//            assertThat(match.stream().anyMatch(r -> r.getStatus().equals("Down"))).isTrue();
//        }
//    }
//
//    private void validateReplicaMapping(TokenRangeReplicasResponse mappingResponse,
//                                        List<IUpgradeableInstance> newInstances,
//                                        Map<String, Map<Range<BigInteger>, List<String>>> expectedRangeMappings)
//    {
//        List<String> transientNodeAddresses = newInstances.stream().map(i -> {
//            InetSocketAddress address = i.config().broadcastAddress();
//            return address.getAddress().getHostAddress() +
//                   ":" +
//                   address.getPort();
//        }).collect(Collectors.toList());
//
//        Set<String> writeReplicaInstances = instancesFromReplicaSet(mappingResponse.writeReplicas());
//        Set<String> readReplicaInstances = instancesFromReplicaSet(mappingResponse.readReplicas());
//        assertThat(readReplicaInstances).doesNotContainAnyElementsOf(transientNodeAddresses);
//        assertThat(writeReplicaInstances).containsAll(transientNodeAddresses);
//
//        validateWriteReplicaMappings(mappingResponse.writeReplicas(), expectedRangeMappings);
//    }
//
//    /**
//     * Generates expected token range and replica mappings specific to the test case involving a 5 node cluster
//     * with the last node replaced with a new node
//     *
//     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
//     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
//     * eg.
//     * Range 1 - A, B, C
//     * Range 2 - B, C, D
//     *
//     * Ranges will have [RF] replicas with ranges containing the replacement node having [RF + no. replacement nodes].
//     *
//     * eg.
//     * Range 1 - A, B, C
//     * Range 2 - B, C, D (with D being replaced with E)
//     * Expected Range 2 - B, C, D, E (With E taking over the range of the node being replaced)
//     */
//    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingNodeReplacement()
//    {
//        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
//        int nodeCount = annotation.nodesPerDc()  * annotation.numDcs();
//        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(nodeCount);
//        Map<Range<BigInteger>, List<String>> mapping = new HashMap<>();
//        mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1:7012", "127.0.0.2:7012", "127.0.0.3:7012"));
//        mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2:7012", "127.0.0.3:7012", "127.0.0.4:7012"));
//        mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3:7012", "127.0.0.4:7012", "127.0.0.5:7012",
//                                                         "127.0.0.6:7012"));
//        mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4:7012", "127.0.0.5:7012", "127.0.0.1:7012",
//                                                         "127.0.0.6:7012"));
//        mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5:7012", "127.0.0.1:7012", "127.0.0.2:7012",
//                                                         "127.0.0.6:7012"));
//        mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1:7012", "127.0.0.2:7012", "127.0.0.3:7012"));
//        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
//        {
//            {
//                put("datacenter1", mapping);
//            }
//        };
//    }
//
//    /**
//     * Generates expected token range and replica mappings specific to the test case involving a 10 node cluster
//     * across 2 DCs with the last 2 nodes leaving the cluster (1 per DC), with RF 3
//     *
//     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
//     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
//     * eg.
//     * Range 1 - A, B, C
//     * Range 2 - B, C, D
//     *
//     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
//     * to allow per-DC validation as returned from the sidecar endpoint.
//     *
//     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
//     * the new replicas being the existing nodes in ring-order.
//     *
//     * eg.
//     * Range 1 - A, B, C
//     * Range 2 - B, C, D (with D being the leaving node)
//     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
//     */
//    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingReplacementMultiDC()
//    {
//        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
//        int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
//        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(nodeCount);
//        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
//        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();
//
//        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1:7012", "127.0.0.3:7012", "127.0.0.5:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2:7012", "127.0.0.4:7012", "127.0.0.6:7012"));
//
//        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3:7012", "127.0.0.5:7012", "127.0.0.7:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2:7012", "127.0.0.4:7012", "127.0.0.6:7012"));
//
//        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3:7012", "127.0.0.5:7012", "127.0.0.7:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.4:7012", "127.0.0.6:7012", "127.0.0.8:7012"));
//
//        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5:7012", "127.0.0.7:7012", "127.0.0.9:7012"));
//        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4:7012", "127.0.0.6:7012", "127.0.0.8:7012"));
//
//        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5:7012", "127.0.0.7:7012", "127.0.0.9:7012"));
//        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.6:7012", "127.0.0.8:7012", "127.0.0.10:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.7:7012", "127.0.0.9:7012", "127.0.0.1:7012"));
//        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6:7012", "127.0.0.8:7012", "127.0.0.10:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7:7012", "127.0.0.9:7012", "127.0.0.1:7012"));
//        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.8:7012", "127.0.0.10:7012", "127.0.0.2:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.9:7012", "127.0.0.1:7012", "127.0.0.3:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.8:7012", "127.0.0.10:7012", "127.0.0.2:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9:7012", "127.0.0.1:7012", "127.0.0.3:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.10:7012", "127.0.0.2:7012", "127.0.0.4:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1:7012", "127.0.0.3:7012", "127.0.0.5:7012",
//                                                            "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.10:7012", "127.0.0.2:7012", "127.0.0.4:7012",
//                                                            "127.0.0.12:7012"));
//
//        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1:7012", "127.0.0.3:7012", "127.0.0.5:7012",
//                                                             "127.0.0.11:7012"));
//        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.2:7012", "127.0.0.4:7012", "127.0.0.6:7012"));
//
//        Map<String, Map<Range<BigInteger>, List<String>>> multiDCMapping
//        = new HashMap<String, Map<Range<BigInteger>, List<String>>>()
//        {
//            {
//                put("datacenter1", dc1Mapping);
//                put("datacenter2", dc2Mapping);
//            }
//        };
//        return multiDCMapping;
//    }
//
//}
