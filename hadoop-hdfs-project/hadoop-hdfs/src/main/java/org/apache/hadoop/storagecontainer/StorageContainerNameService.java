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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Namesystem implementation to be used by StorageContainerManager.
 */
public class StorageContainerNameService implements Namesystem {

  private ReentrantReadWriteLock coarseLock = new ReentrantReadWriteLock();
  private String blockPoolId;
  private volatile boolean serviceRunning = true;

  public void shutdown() {
    serviceRunning = false;
  }

  @Override
  public boolean isRunning() {
    return serviceRunning;
  }

  @Override
  public void checkSuperuserPrivilege() throws AccessControlException {
    // TBD
  }

  @Override
  public String getBlockPoolId() {
    return blockPoolId;
  }

  public void setBlockPoolId(String id) {
    this.blockPoolId = id;
  }

  @Override
  public boolean isInStandbyState() {
    // HA mode is not supported
    return false;
  }

  @Override
  public boolean isGenStampInFuture(Block block) {
    // HA mode is not supported
    return false;
  }

  @Override
  public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal) {
    // TBD
  }

  @Override
  public void checkOperation(NameNode.OperationCategory read)
    throws StandbyException {
    // HA mode is not supported
  }

  @Override
  public boolean isInSnapshot(BlockInfoUnderConstruction blockUC) {
    // Snapshots not supported
    return false;
  }

  @Override
  public CacheManager getCacheManager() {
    // Cache Management is not supported
    return null;
  }

  @Override
  public void readLock() {
    coarseLock.readLock().lock();
  }

  @Override
  public void readUnlock() {
    coarseLock.readLock().unlock();
  }

  @Override
  public boolean hasReadLock() {
    return coarseLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  @Override
  public void writeLock() {
    coarseLock.writeLock().lock();
  }

  @Override
  public void writeLockInterruptibly() throws InterruptedException {
    coarseLock.writeLock().lockInterruptibly();
  }

  @Override
  public void writeUnlock() {
    coarseLock.writeLock().unlock();
  }

  @Override
  public boolean hasWriteLock() {
    return coarseLock.isWriteLockedByCurrentThread();
  }

  @Override
  public void checkSafeMode() {
    // TBD
  }

  @Override
  public boolean isInSafeMode() {
    return false;
  }

  @Override
  public boolean isInStartupSafeMode() {
    return false;
  }

  @Override
  public boolean isPopulatingReplQueues() {
    return false;
  }

  @Override
  public void incrementSafeBlockCount(int replication) {
    // Do nothing
  }

  @Override
  public void decrementSafeBlockCount(BlockInfo b) {
    // Do nothing
  }
}
