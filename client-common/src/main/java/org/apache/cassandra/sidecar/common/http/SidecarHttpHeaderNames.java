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

package org.apache.cassandra.sidecar.common.http;

/**
 * Custom header names for sidecar
 */
public final class SidecarHttpHeaderNames
{
    /**
     * {@code "cassandra-content-xxhash32"}
     */
    public static final String CONTENT_XXHASH32 = "cassandra-content-xxhash32";
    /**
     * {@code "cassandra-content-xxhash32-seed"}
     */
    public static final String CONTENT_XXHASH32_SEED = "cassandra-content-xxhash32-seed";

    /**
     * {@code "cassandra-sidecar-job-uuid"}
     */
    public static final String ASYNC_JOB_UUID = "cassandra-sidecar-job-uuid";
}
