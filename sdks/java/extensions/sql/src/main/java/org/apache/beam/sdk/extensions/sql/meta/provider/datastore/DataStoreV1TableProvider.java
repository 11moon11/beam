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
package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * {@link TableProvider} for {@link DataStoreV1Table}.
 *
 * <p>A sample of DataStoreV1Table table is:
 *
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   name VARCHAR,
 *   favorite_color VARCHAR,
 *   favorite_numbers ARRAY<INTEGER>
 * )
 * TYPE 'datastoreV1'
 * LOCATION 'projectId/kind'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class DataStoreV1TableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "datastoreV1";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new DataStoreV1Table(table);
  }
}
