/**
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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;

/**
 * A factory for creating {@link FsDatasetImpl} objects.
 */
public class FsDatasetFactory extends FsDatasetSpi.Factory<FsDatasetImpl> {

  private final Map<NodeType, FsDatasetImpl> datasetMap = new HashMap<>();

  @Override
  public synchronized FsDatasetImpl newInstance(DataNode datanode,
      DataStorage storage, Configuration conf,
      NodeType serviceType) throws IOException {
    FsDatasetImpl dataset = datasetMap.get(serviceType);
    if (dataset != null) {
      return dataset;
    }
    switch (serviceType) {
    case NAME_NODE:
      dataset = new FsDatasetImpl(datanode, storage, conf);
      break;
    default:
      throw new IllegalArgumentException("Unsupported node type " + serviceType);
    }
    datasetMap.put(serviceType, dataset);
    return dataset;
  }
}
