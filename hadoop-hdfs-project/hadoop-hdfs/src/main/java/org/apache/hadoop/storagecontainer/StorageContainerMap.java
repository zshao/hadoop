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

package org.apache.hadoop.storagecontainer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.util.GSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Maps a storage container to its location on datanodes. Similar to
 * {@link org.apache.hadoop.hdfs.server.blockmanagement.BlocksMap}
 */
public class StorageContainerMap implements GSet<Block, BlockInfo> {

  private Map<Long, BitWiseTrieContainerMap> containerPrefixMap
      = new HashMap<Long, BitWiseTrieContainerMap>();
  private int size;
  public static final int PREFIX_LENGTH = 28;

  @Override
  public int size() {
    // TODO: update size when new containers created
    return size;
  }

  @Override
  public boolean contains(Block key) {
    return getBlockInfoContiguous(key.getBlockId()) != null;
  }

  @Override
  public BlockInfoContiguous get(Block key) {
    return getBlockInfoContiguous(key.getBlockId());
  }

  @Override
  public BlockInfoContiguous put(BlockInfo element) {
    BlockInfoContiguous info = getBlockInfoContiguous(element.getBlockId());
    if (info == null) {
      throw new IllegalStateException(
          "The containers are created by splitting");
    }
    // TODO: replace
    return info;
  }

  @Override
  public BlockInfoContiguous remove(Block key) {
    // It doesn't remove
    return getBlockInfoContiguous(key.getBlockId());
  }

  @Override
  public void clear() {
    containerPrefixMap.clear();
  }

  @Override
  public Iterator<BlockInfo> iterator() {
    // TODO : Support iteration
    throw new UnsupportedOperationException("");
  }

  /**
   * Initialize a new trie for a new bucket.
   */
  public synchronized void initPrefix(long prefix) {
    Preconditions.checkArgument((prefix >>> PREFIX_LENGTH) == 0,
        "Prefix shouldn't be longer than "+PREFIX_LENGTH+" bits");
    if (getTrieMap(prefix << (64 - PREFIX_LENGTH)) != null) {
      // Already initialized
      return;
    }
    BitWiseTrieContainerMap newTrie = new BitWiseTrieContainerMap(prefix,
        PREFIX_LENGTH);
    containerPrefixMap.put(prefix, newTrie);
  }

  @VisibleForTesting
  synchronized BitWiseTrieContainerMap getTrieMap(long containerId) {
    long prefix = containerId >>> (64 - PREFIX_LENGTH);
    return containerPrefixMap.get(prefix);
  }

  @VisibleForTesting
  BlockInfoContiguous getBlockInfoContiguous(long containerId) {
    BitWiseTrieContainerMap map = getTrieMap(containerId);
    if (map == null) {
      return null;
    }
    return map.get(containerId);
  }

  public void splitContainer(long key) {
    BitWiseTrieContainerMap map = getTrieMap(key);
    if (map == null) {
      throw new IllegalArgumentException("No container exists");
    }
    map.addBit(key);
  }
}
