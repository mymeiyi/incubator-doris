package org.apache.doris.transaction;

import org.apache.doris.catalog.Table;
import org.apache.doris.thrift.TTabletCommitInfo;

import java.util.List;

public class TxnInfo {
    long txnId;
    Table table;
    List<TTabletCommitInfo> commitInfos;

    public TxnInfo(long txnId, Table table, List<TTabletCommitInfo> commitInfos) {
        this.txnId = txnId;
        this.table = table;
        this.commitInfos = commitInfos;
    }

    public long getTxnId() {
        return txnId;
    }

    public Table getTable() {
        return table;
    }

    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }
}
