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

package org.apache.doris.transaction;

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";
    private static String fileName2 = "./TransactionStateTest2";

    @After
    public void tearDown() {
        new File(fileName).delete();
        new File(fileName2).delete();
    }

    @Test
    public void testSerDe() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        transactionState.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TransactionState readTransactionState = TransactionState.read(in);

        Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
        in.close();
    }

    @Test
    public void testSerDeWithTxnCommitAttachment() throws IOException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File(fileName2);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"),
                TransactionStatus.COMMITTED, "", 100, 50000L,
                new LoadJobFinalOperation(1000L, null, 0, 0, 0, JobState.FINISHED, null),
                100, 200, 300, 400);

        transactionState.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        TransactionState readTransactionState = TransactionState.read(in);

        Assert.assertEquals(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB,
                readTransactionState.getTxnCommitAttachment().sourceType);
        Assert.assertTrue(readTransactionState.getTxnCommitAttachment() instanceof LoadJobFinalOperation);
        LoadJobFinalOperation loadJobFinalOperation
                = (LoadJobFinalOperation) (readTransactionState.getTxnCommitAttachment());
        Assert.assertEquals(1000L, loadJobFinalOperation.getId());
        in.close();
    }
}
