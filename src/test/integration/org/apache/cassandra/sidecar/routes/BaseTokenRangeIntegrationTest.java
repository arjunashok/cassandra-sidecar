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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.Uninterruptibles;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.adapters.base.Partitioner;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the token range replica mapping endpoint with cassandra container.
 */
public class BaseTokenRangeIntegrationTest extends IntegrationTestBase
{

    protected void validateTokenRanges(TokenRangeReplicasResponse mappingsResponse,
                                       List<Range<BigInteger>> expectedRanges)
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicaSet = mappingsResponse.writeReplicas();
        List<TokenRangeReplicasResponse.ReplicaInfo> readReplicaSet = mappingsResponse.readReplicas();
        List<Range<BigInteger>> writeRanges = writeReplicaSet.stream()
                                                             .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                        new BigInteger(r.end())))
                                                             .collect(Collectors.toList());

        List<Range<BigInteger>> readRanges = readReplicaSet.stream()
                                                           .map(r -> Range.openClosed(new BigInteger(r.start()),
                                                                                      new BigInteger(r.end())))
                                                           .collect(Collectors.toList());


        assertThat(writeRanges.size()).isEqualTo(writeReplicaSet.size());
        assertThat(writeRanges).containsExactlyElementsOf(expectedRanges);

        //Sorted and Overlap check
        validateOrderAndOverlaps(writeRanges);
        validateOrderAndOverlaps(readRanges);
    }

    private void validateOrderAndOverlaps(List<Range<BigInteger>> ranges)
    {
        for (int r = 0; r < ranges.size() - 1; r++)
        {
            assertThat(ranges.get(r).upperEndpoint()).isLessThan(ranges.get(r + 1).upperEndpoint());
            assertThat(ranges.get(r).intersection(ranges.get(r + 1)).isEmpty()).isTrue();
        }
    }

    protected void validateNodeStates(TokenRangeReplicasResponse mappingResponse,
                                      Set<String> dcReplication,
                                      Function<Integer, String> statusFunction)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int expectedReplicas = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * dcReplication.size();

        AbstractCassandraTestContext cassandraTestContext = sidecarTestContext.cassandraTestContext();
        assertThat(mappingResponse.replicaState().size()).isEqualTo(expectedReplicas);
        for (int i = 1; i <= cassandraTestContext.cluster().size(); i++)
        {
            IInstanceConfig config = cassandraTestContext.cluster().get(i).config();

            if (dcReplication.contains(config.localDatacenter()))
            {
                String ipAndPort = config.broadcastAddress().getAddress().getHostAddress() + ":"
                                   + config.broadcastAddress().getPort();

                String expectedStatus = statusFunction.apply(i);
                assertThat(mappingResponse.replicaState().get(ipAndPort)).isEqualTo(expectedStatus);
            }
        }
    }

    protected UpgradeableCluster getMultiDCCluster(int numNodes,
                                                   int numDcs,
                                                   BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier mdcTokenSupplier =
        MultiDcTokenSupplier.evenlyDistributedTokens(numNodes,
                                                     numDcs,
                                                     1);

        int totalNodeCount = annotation.nodesPerDc() * annotation.numDcs();
        return cassandraTestContext.configureAndStartCluster(
        builder -> {
            builder.withInstanceInitializer(initializer);
            builder.withTokenSupplier(mdcTokenSupplier);
            builder.withNodeIdTopology(networkTopology(totalNodeCount,
                                                       (nodeId) -> nodeId % 2 != 0 ?
                                                                   dcAndRack("datacenter1", "rack1") :
                                                                   dcAndRack("datacenter2", "rack2")));
        });
    }

    protected List<Range<BigInteger>> generateExpectedRanges()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int finalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        TokenSupplier tokenSupplier = (annotation.numDcs() > 1) ?
                                      MultiDcTokenSupplier.evenlyDistributedTokens(
                                      annotation.nodesPerDc() + annotation.newNodesPerDc(),
                                      annotation.numDcs(),
                                      1) :
                                      TokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc() +
                                                                            annotation.newNodesPerDc(),
                                                                            1);

        List<Range<BigInteger>> expectedRanges = new ArrayList<>();
        BigInteger startToken = Partitioner.Murmur3.minToken;
        BigInteger endToken = Partitioner.Murmur3.maxToken;
        int node = 1;
        BigInteger prevToken = new BigInteger(tokenSupplier.tokens(node++).stream().findFirst().get());
        Range<BigInteger> firstRange = Range.openClosed(startToken, prevToken);
        expectedRanges.add(firstRange);
        while (node <= finalNodeCount)
        {
            BigInteger currentToken = new BigInteger(tokenSupplier.tokens(node).stream().findFirst().get());
            expectedRanges.add(Range.openClosed(prevToken, currentToken));
            prevToken = currentToken;
            node++;
        }
        expectedRanges.add(Range.openClosed(prevToken, endToken));
        return expectedRanges;
    }

    protected Set<String> instancesFromReplicaSet(List<TokenRangeReplicasResponse.ReplicaInfo> replicas)
    {
        return replicas.stream()
                       .map(r -> r.replicasByDatacenter().values())
                       .flatMap(Collection::stream)
                       .flatMap(Collection::stream)
                       .collect(Collectors.toSet());
    }

    void retrieveMappingWithKeyspace(VertxTestContext context, String keyspace,
                                     Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/" + keyspace + "/token-range-replicas";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .send(context.succeeding(verifier));
        });
    }

    void assertMappingResponseOK(TokenRangeReplicasResponse mappingResponse)
    {
        assertMappingResponseOK(mappingResponse, 1, Collections.singleton("datacenter1"));
    }

    void assertMappingResponseOK(TokenRangeReplicasResponse mappingResponse,
                                 int replicationFactor,
                                 Set<String> dcReplication)
    {
        assertThat(mappingResponse).isNotNull();
        assertThat(mappingResponse.readReplicas()).isNotNull();
        assertThat(mappingResponse.writeReplicas()).isNotNull();
        TokenRangeReplicasResponse.ReplicaInfo readReplica = mappingResponse.readReplicas().get(0);
        assertThat(readReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());
        TokenRangeReplicasResponse.ReplicaInfo writeReplica = mappingResponse.writeReplicas().get(0);
        assertThat(writeReplica.replicasByDatacenter()).isNotNull().hasSize(dcReplication.size());

        for (String dcName : dcReplication)
        {
            assertThat(readReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(readReplica.replicasByDatacenter().get(dcName)).isNotNull().hasSize(replicationFactor);

            assertThat(writeReplica.replicasByDatacenter().keySet()).isNotEmpty().contains(dcName);
            assertThat(writeReplica.replicasByDatacenter().get(dcName)).isNotNull();
            assertThat(writeReplica.replicasByDatacenter().get(dcName).size())
            .isGreaterThanOrEqualTo(replicationFactor);
        }
    }

    /**
     * ByteBuddy helper for a single joining node
     */
    @Shared
    public static class BBHelperSingleJoiningNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with 1 joining node
            // We intercept the bootstrap of the leaving node (4) to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperSingleJoiningNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperMultipleJoiningNodes
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with a 2 joining nodes
            // We intercept the joining of nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultipleJoiningNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for doubling cluster size
     */
    @Shared
    public static class BBHelperDoubleClusterSize
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(5);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(5);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster doubling in size
            // We intercept the bootstrap of the new nodes (6-10) to validate token ranges
            if (nodeNumber > 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperDoubleClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy Helper for a single leaving node
     */
    @Shared
    public static class BBHelperSingleLeavingNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (5) to validate token ranges
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperSingleLeavingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes
     */
    @Shared
    public static class BBHelperMultipleLeavingNodes
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a 2 leaving nodes
            // We intercept the shutdown of the leaving nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperMultipleLeavingNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMovingNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == 5)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return res;
        }
    }

    /**
     * ByteBuddy helper for a single node replacement
     */
    @Shared
    public static class BBHelperReplacementsNode
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a replacement node
            // We intercept the bootstrap of the replacement (6th) node to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for shrinking cluster by half its size
     */
    @Shared
    public static class BBHelperHalveClusterSize
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(3);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(3);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 6 node cluster
            // We intercept the shutdown of the removed nodes (4-6) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterSize.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperDoubleClusterMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(6);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves doubling the size of a 6 node cluster (3 per DC)
            // We intercept the bootstrap of nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperDoubleClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes multi-DC
     */
    @Shared
    public static class BBHelperJoiningNodesMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 6 node cluster (3 nodes per DC) with a 2 joining nodes (1 per DC)
            // We intercept the bootstrap of nodes (7, 8) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperJoiningNodesMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for halve cluster size with multi-DC
     */
    @Shared
    public static class BBHelperHalveClusterMultiDC
    {
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(6);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 12 node cluster (6 per DC)
            // We intercept the shutdown of the removed nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            orig.call();
        }
    }

    /**
     * ByteBuddy helper for multi-DC node replacement
     */
    @Shared
    public static class BBHelperReplacementsMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        public static final CountDownLatch NODE_START = new CountDownLatch(1);
        public static final CountDownLatch TRANSIENT_STATE_START = new CountDownLatch(2);
        public static final CountDownLatch TRANSIENT_STATE_END = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (across 2 DCs) with a 2 replacement nodes
            // We intercept the bootstrap of the replacement nodes to validate token ranges
            if (nodeNumber > 10)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            NODE_START.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            TRANSIENT_STATE_START.countDown();
            Uninterruptibles.awaitUninterruptibly(TRANSIENT_STATE_END);
            return result;
        }
    }
}
