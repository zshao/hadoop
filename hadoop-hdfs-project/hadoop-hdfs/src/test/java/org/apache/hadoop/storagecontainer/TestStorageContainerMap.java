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

import org.apache.hadoop.storagecontainer.protocol.StorageContainer;
import org.junit.Assert;
import org.junit.Test;

public class TestStorageContainerMap {

  @Test
  public void testTrieMap() {
    BitWiseTrieContainerMap trie = new BitWiseTrieContainerMap(0x2345fab, 28);
    long key = 0x2345fab123456679L;
    Assert.assertEquals(0x2345fab000000000L, trie.get(key).getBlockId());
    trie.addBit(key);
    Assert.assertEquals(0x2345fab000000001L, trie.get(key).getBlockId());
    trie.addBit(key);
    Assert.assertEquals(0x2345fab000000001L, trie.get(key).getBlockId());
    trie.addBit(key);
    Assert.assertEquals(0x2345fab000000001L, trie.get(key).getBlockId());
    trie.addBit(key);
    Assert.assertEquals(0x2345fab000000009L, trie.get(key).getBlockId());
    for (int i = 0; i < 32; i++) {
      trie.addBit(key);
    }
    Assert.assertEquals(0x2345fab123456679L, trie.get(key).getBlockId());
  }

  @Test
  public void testTrieMapNegativeKey() {
    BitWiseTrieContainerMap trie = new BitWiseTrieContainerMap(0xf345fab, 28);
    long key = 0xf345fab123456679L;
    for (int i = 0; i < 13; i++) {
      trie.addBit(key);
    }
    Assert.assertEquals(0xf345fab000000679L, trie.get(key).getBlockId());
  }

  @Test
  public void testStorageContainer() {
    StorageContainerMap containerMap = new StorageContainerMap();
    try {
      containerMap.initPrefix(0xabcdef12);
      Assert.fail("Prefix is longer than expected");
    } catch (IllegalArgumentException ex) {
      // Expected
    }
    containerMap.initPrefix(0xabcdef1);
    containerMap.initPrefix(0xfffffff);
    Assert.assertTrue(null == containerMap.getTrieMap(0xabcdef2));
    containerMap.splitContainer(0xabcdef1111122223L);
    containerMap.splitContainer(0xabcdef1111122223L);
    StorageContainer sc = new StorageContainer(0xabcdef1000000003L);
    Assert.assertEquals(0xabcdef1000000003L, containerMap.get(sc).getBlockId());
    Assert.assertEquals(0xabcdef1000000003L,
        containerMap.getTrieMap(0xabcdef1111122223L)
            .get(0xabcdef1111122223L).getBlockId());
  }

  @Test
  public void testStorageContainerMaxSplit() {
    StorageContainerMap containerMap = new StorageContainerMap();
    containerMap.initPrefix(0xabcdef1);
    for (int i = 0; i < 64 - StorageContainerMap.PREFIX_LENGTH; i++) {
      containerMap.splitContainer(0xabcdef1111122223L);
    }
    Assert.assertEquals(0xabcdef1111122223L,
        containerMap.getTrieMap(0xabcdef1111122223L)
            .get(0xabcdef1111122223L).getBlockId());
    try {
      containerMap.splitContainer(0xabcdef1111122223L);
      Assert.fail("Exceeding max splits");
    } catch (IllegalArgumentException expected) {}
  }
}