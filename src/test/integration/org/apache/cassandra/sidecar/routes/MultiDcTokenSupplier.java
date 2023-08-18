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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.api.TokenSupplier;

/**
 * Static factory holder that provides a multi-DC token supplier
 */
public class MultiDcTokenSupplier
{

    static TokenSupplier evenlyDistributedTokens(int numNodes, int numDcs, int numTokens)
    {
        long totalTokens = (long) numNodes * numDcs * numTokens;
        BigInteger increment = BigInteger.valueOf((Long.MAX_VALUE / totalTokens) * 4);
        List<String>[] tokens = new List[numNodes * numDcs];

        for (int i = 0; i < (numNodes * numDcs); ++i)
        {
            tokens[i] = new ArrayList(numTokens);
        }

        BigInteger value = BigInteger.valueOf(Long.MIN_VALUE + 1);

        for (int i = 0; i < numTokens; ++i)
        {
            int nodeId = 1;
            while (nodeId <= (numNodes * numDcs))
            {
                value = value.add(increment);
                for (int dc = 0; dc < numDcs; dc++)
                {
                    tokens[nodeId - 1].add(value.add(BigInteger.valueOf(dc)).toString());
                    nodeId++;
                }
            }
        }

        return (nodeIdx) -> {
            return tokens[nodeIdx - 1];
        };
    }
}
