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

/**
 * Holder class for Node related
 */
public class NodeInfo
{
    /**
     * Represents the known states of a node
     */
    public enum NodeState
    {
        JOINING("Joining"),
        LEAVING("Leaving"),
        MOVING("Moving"),
        NORMAL("Normal"),
        REPLACING("Replacing");

        @Override
        public String toString()
        {
            return state;
        }
        private final String state;

        NodeState(String state)
        {
            this.state = state;
        }
    }

    /**
     * Represents the statuses a node can have
     */
    public enum NodeStatus
    {
        UP("Up"),
        DOWN("Down");

        @Override
        public String toString()
        {
            return status;
        }
        private final String status;
        NodeStatus(String status)
        {
            this.status = status;
        }
    }
}
