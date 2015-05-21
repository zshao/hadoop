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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.storagecontainer.protocol.StorageContainer;

/**
 * A simple trie implementation.
 * A Storage Container Identifier can be broken into a prefix followed by
 * a container id. The id is represented in the trie for efficient prefix
 * matching. At any trie node, the left child represents 0 bit and the right
 * child represents the 1 bit. The storage containers exist only at the leaves.
 */
public class BitWiseTrieContainerMap {

  private final int prefixLength;
  private final long constantPrefix;

  public BitWiseTrieContainerMap(long constantPrefix, int prefixLength) {
    if (prefixLength > 64 || prefixLength <= 0) {
      throw new IllegalArgumentException("Must have 0<keyLength<=64");
    }
    this.prefixLength = prefixLength;
    this.constantPrefix = constantPrefix;
    root = new TrieNode();
    root.bitLength = 0;
    long rootContainerId = constantPrefix << (64 - prefixLength);

    // TODO: Fix hard coded replication
    root.container = new BlockInfoContiguous(
        new StorageContainer(rootContainerId), (short)3);
  }

  public synchronized BlockInfoContiguous get(long key) {
    if (root.left == null || root.right == null) {
      return root.container;
    }
    return getInternal(key, root, 1).container;
  }

  /**
   * Splits the trie node corresponding to the key into left and right
   * children.
   * TODO: This should be idempotent
   */
  public synchronized void addBit(long key) {
    TrieNode tn = root;
    if (root.left != null || root.right != null) {
      tn = getInternal(key, root, 1);
    }

    Preconditions.checkArgument(tn.bitLength < (64 - prefixLength),
        "Maximum bitLength achieved for key "+key);

    Preconditions.checkArgument(tn.right == null && tn.left == null,
        "The extension of trie is allowed only at the leaf nodes");

    // Container Id of the left child is same as that of parent, but
    // the bitLength increases by one.
    tn.left = new TrieNode(tn.container, tn.bitLength+1);

    long rightContainerId = tn.container.getBlockId() | (0x1L << tn.bitLength);

    // TODO: Fix hard coded replication
    BlockInfoContiguous newContainer = new BlockInfoContiguous(
        new StorageContainer(rightContainerId), (short) 3);

    tn.right = new TrieNode(newContainer, tn.bitLength+1);
    tn.container = null;
  }

  public long getConstantPrefix() {
    return constantPrefix;
  }

  /**
   * Performs bitwise matching recursively down the trie.
   * The bitIndex is counted from left. The maximum depth of recursion
   * is 64 - {@link
   * org.apache.hadoop.storagecontainer.StorageContainerMap#PREFIX_LENGTH}
   */
  private TrieNode getInternal(long key, TrieNode head, int bitLength) {
    long mask = 0x1L << (bitLength-1);

    TrieNode nextNode;
    nextNode = (key & mask) == 0 ? head.left : head.right;
    if (nextNode == null) {
      return head;
    } else {
      return getInternal(key, nextNode, bitLength+1);
    }
  }

  private static class TrieNode {
    private TrieNode left = null;
    private TrieNode right = null;
    private BlockInfoContiguous container;

    private int bitLength;

    TrieNode() {}

    TrieNode(BlockInfoContiguous c, int l) {
      this.container = c;
      this.bitLength = l;
    }
  }

  private TrieNode root;

}
