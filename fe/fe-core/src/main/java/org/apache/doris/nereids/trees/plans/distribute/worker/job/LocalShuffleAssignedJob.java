// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * LocalShuffleAssignedJob:
 *   this instance will use ignore_data_distribution function of local shuffle to add parallel
 *   after scan data
 */
public class LocalShuffleAssignedJob extends StaticAssignedJob {
    public final int shareScanId;
    public final boolean receiveDataFromLocal;

    public LocalShuffleAssignedJob(
            int indexInUnassignedJob, int shareScanId, boolean receiveDataFromLocal, TUniqueId instanceId,
            UnassignedJob unassignedJob,
            DistributedPlanWorker worker, ScanSource scanSource) {
        super(indexInUnassignedJob, instanceId, unassignedJob, worker, scanSource);
        this.shareScanId = shareScanId;
        this.receiveDataFromLocal = receiveDataFromLocal;
    }

    @Override
    protected Map<String, String> extraInfo() {
        return ImmutableMap.of("shareScanIndex", String.valueOf(shareScanId));
    }

    @Override
    protected String formatScanSourceString() {
        if (receiveDataFromLocal) {
            return "read data from first instance of " + getAssignedWorker();
        } else {
            return super.formatScanSourceString();
        }
    }
}