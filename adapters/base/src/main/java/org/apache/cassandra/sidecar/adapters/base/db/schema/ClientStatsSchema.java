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

package org.apache.cassandra.sidecar.adapters.base.db.schema;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.common.server.data.TableSchema;
import org.jetbrains.annotations.NotNull;

/**
 * Holds the prepared statements for operations related to client connection stats retrieved from
 * the "clients" virtual table
 */
public class ClientStatsSchema extends TableSchema
{

    private static final String TABLE_NAME = "clients";
    private static final String KEYSPACE_NAME = "system_views";

    private PreparedStatement listAll;
    private PreparedStatement selectUserConnections;

    @Override
    protected String keyspaceName()
    {
        return KEYSPACE_NAME;
    }

    public void prepareStatements(@NotNull Session session)
    {
        listAll = prepare(listAll, session, ClientStatsSchema.listAllStatement());
        selectUserConnections = prepare(selectUserConnections, session, ClientStatsSchema.selectConnectionsByUserStatement());
    }

    @Override
    protected boolean initializeInternal(@NotNull Session session)
    {
        prepareStatements(session);
        return true;
    }

    @Override
    protected String createSchemaStatement()
    {
        return null;
    }

    @Override
    protected String tableName()
    {
        return TABLE_NAME;
    }

    public PreparedStatement listAll()
    {
        return listAll;
    }

    public PreparedStatement connectionCountByUser()
    {
        return selectUserConnections;
    }

    static String listAllStatement()
    {
        return String.format("SELECT address, " +
                             "port, " +
                             "hostname, " +
                             "username, " +
                             "connection_stage, " +
                             "protocol_version, " +
                             "driver_name, " +
                             "driver_version, " +
                             "ssl_enabled, " +
                             "ssl_protocol, " +
                             "ssl_cipher_suite " +
                             "FROM %s.%s;",
                             KEYSPACE_NAME,
                             TABLE_NAME);
    }

    static String selectConnectionsByUserStatement()
    {
        return String.format("SELECT username, COUNT(*) AS connection_count " +
                             "FROM %s.%s;",
                             KEYSPACE_NAME,
                             TABLE_NAME);
    }
}