/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package xdcrSelfCheck.scenarios;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import xdcrSelfCheck.ClientPayloadProcessor;
import xdcrSelfCheck.ClientThread;

import java.util.Arrays;
import java.util.List;

public abstract class ClientConflictRunner {

    public enum ConflictType {
        II_CV,    // insert-insert constraint violation
        IU_CV,    // insert-update constraint violation
        UU_CV,    // update-update constraint violation
        UU_TM,    // update-update timestamp mismatch
        UD_TM_MR, // update-delete timestamp mismatch, updating side
                  // update-delete missing row, deleting side
        DD_MR;    // delete-delete missing row

        public static ConflictType fromOrdinal(int val) {
            switch (val) {
                case 0: return ConflictType.II_CV;
                case 1: return ConflictType.IU_CV;
                case 2: return ConflictType.UU_CV;
                case 3: return ConflictType.UU_TM;
                case 4: return ConflictType.UD_TM_MR;
                case 5: return ConflictType.DD_MR;
                default:
                    throw new IllegalArgumentException("Unrecognized conflict type value: " + val);
            }
        }

        public static List<Integer> ordinals() {
            return Arrays.asList(0, 1, 2, 3, 4, 5);
        }
    }

    protected final ClientThread clientThread;
    protected final Client primaryClient;
    protected final Client secondaryClient;
    protected final ClientPayloadProcessor processor;
    protected final byte cid;
    protected final long rid;


    protected static VoltLogger LOG = new VoltLogger(ClientConflictRunner.class.getSimpleName());
    private final static String PARTITIONED_TABLE= "xdcr_partitioned";
    private final static String REPLICATED_TABLE= "xdcr_replicated";
    private final static String APPLYBINARYLOGSP_PROC= "ApplyBinaryLogSP";
    private final static String APPLYBINARYLOGMP_PROC= "ApplyBinaryLogMP";

    protected ClientConflictRunner(ClientThread clientThread) {
        this.clientThread = clientThread;
        primaryClient = clientThread.getPrimaryClient();
        secondaryClient = clientThread.getSecondaryClient();
        processor = clientThread.getClientPayloadProcessor();
        cid = clientThread.getCid();
        rid = clientThread.getNextRid().get();
    }

    public static ClientConflictRunner getInstance(ClientThread clientThread, ConflictType conflictType) {
        switch (conflictType) {
            case II_CV:
                return new IICVConflictRunner(clientThread);
            case IU_CV:
                return new IUCVConflictRunner(clientThread);
            case UU_CV:
                return new UUCVConflictRunner(clientThread);
            case UU_TM:
                return new UUTMConflictRunner(clientThread);
            case UD_TM_MR:
                return new UDTMMRConflictRunner(clientThread);
            case DD_MR:
                return new DDMRConflictRunner(clientThread);
            default:
                throw new IllegalArgumentException("Unrecognized conflict conflictType: " + conflictType.name());
        }
    }

    static long getProcCallCounts(Client client, String applyBinaryCall) throws Exception {
        ClientResponse cr = client.callProcedure("@Statistics", "PROCEDURE", 0);
        VoltTable results = cr.getResults()[0];

        while (results.advanceRow()) {
            String procName = results.getString("PROCEDURE");
            long callCount = results.getLong("INVOCATIONS");
            if (procName.contains(applyBinaryCall)) {
                return callCount;
            }
        }

        return 0;
    }

    static void waitForApplyBinaryLog(byte cid, String tableName,
                                      Client primaryClient, long numPrimaryExpected,
                                      Client secondaryClient, long numSecondaryExpected) throws Exception {
        boolean done = false;
        final long start = System.currentTimeMillis();
        while (!done && System.currentTimeMillis() - start < 60000) {
            long primaryCount = primaryClient.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName + " WHERE cid=?;", cid).getResults()[0].asScalarLong();
            long secondaryCount = secondaryClient.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName + " WHERE cid=?;", cid).getResults()[0].asScalarLong();
            done = (primaryCount == numPrimaryExpected && secondaryCount == numSecondaryExpected);
        }
    }

    abstract public void runScenario(String tableName) throws Throwable;

    protected void verifyTableData(String cluster, String tableName, long myRid,
                                   VoltTable voltTable, int rowCount, int rowIdx,
                                   ClientPayloadProcessor.Pair payload) {

        if (voltTable.getRowCount() != rowCount) {
            throw new VoltAbortException(
                    cluster + " " + tableName + " table has more rows than expected: actual " + voltTable.getRowCount() + ", expected " + rowCount);
        }

        VoltTableRow rowData = voltTable.fetchRow(rowIdx);
        byte cid = (byte) rowData.getLong("cid");
        if (Byte.compare(cid, cid) != 0) {
            throw new VoltAbortException(
                    cluster + " " + tableName + " table has mismatched cid: actual " + cid + ", expected " + cid);
        }

        long rid = rowData.getLong("rid");
        if (myRid != rid) {
            throw new VoltAbortException(
                    cluster + " " + tableName + " table has mismatched rid: actual " + rid + ", expected " + this.rid);
        }

        byte[] keys = rowData.getVarbinary("key");
        if (!Arrays.equals(payload.Key.getBytes(), keys)) {
            throw new VoltAbortException(
                    cluster + " " + tableName + " table has mismatched key: actual " + keys + ", expected " + payload.Key);
        }

        byte[] value = rowData.getVarbinary("value");
        if (!Arrays.equals(payload.getStoreValue(), value)) {
            throw new VoltAbortException(
                    cluster + " " + tableName + " table has mismatched value: actual " + keys + ", expected " + payload.Key);
        }
    }

    protected void resetTable(String tableName) throws Exception {
        resetTable(tableName, false);
    }

    protected void resetTable(String tableName, boolean both) throws Exception {
        primaryClient.callProcedure("@AdHoc", "DELETE FROM " + tableName + " WHERE cid=?;", cid);
        if (both) {
            secondaryClient.callProcedure("@AdHoc", "DELETE FROM " + tableName + " WHERE cid=?;", cid);
        }
        waitForApplyBinaryLog(cid, tableName, primaryClient, 0, secondaryClient, 0);
    }

    protected VoltTable[] selectTable(Client client, String tableName) throws Exception {
        return client.callProcedure("@AdHoc", "SELECT * FROM " + tableName + " WHERE cid=?;", cid).getResults();
    }

    protected String getInsertProcCall(String tableName) {
        switch (tableName) {
            case PARTITIONED_TABLE:
                return "InsertPartitionedSP";
            case REPLICATED_TABLE:
                return "InsertReplicatedMP";
            default:
                throw new IllegalArgumentException("Unrecognized table name: " + tableName);
        }
    }

    protected String getUpdateProcCall(String tableName) {
        switch (tableName) {
            case PARTITIONED_TABLE:
                return "UpdatePartitionedSP";
            case REPLICATED_TABLE:
                return "UpdateReplicatedMP";
            default:
                throw new IllegalArgumentException("Unrecognized table name: " + tableName);
        }
    }

    protected String getApplyBinaryLogProcCall(String tableName) {
        switch (tableName) {
            case PARTITIONED_TABLE:
                return APPLYBINARYLOGSP_PROC;
            case REPLICATED_TABLE:
                return APPLYBINARYLOGMP_PROC;
            default:
                throw new IllegalArgumentException("Unrecognized table name: " + tableName);
        }
    }

    protected String getReadProcCall(String tableName) {
        switch (tableName) {
            case PARTITIONED_TABLE:
                return "ReadSP";
            case REPLICATED_TABLE:
                return "ReadMP";
            default:
                throw new IllegalArgumentException("Unrecognized table name: " + tableName);
        }
    }

}
