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

package xdcrSelfCheck.resolves;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.*;
import xdcrSelfCheck.resolves.XdcrConflict.ACTION_TYPE;
import xdcrSelfCheck.resolves.XdcrConflict.CONFLICT_TYPE;
import xdcrSelfCheck.resolves.XdcrConflict.DECISION;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConflictResolveChecker {

    protected Client primaryClient;
    protected Client secondaryClient;
    protected String primaryvoltdbroot;
    protected String secondaryvoltdbroot;

    static final List<String> TABLES_TO_CHECK = Arrays.asList("xdcr_partitioned", "xdcr_replicated");
    static VoltLogger LOG = new VoltLogger(ConflictResolveChecker.class.getSimpleName());

    interface ResolveChecker {
        boolean verifyExpectation(XdcrConflict xdcrExpected, List<XdcrConflict> xdcrActuals) throws JSONException;
    }

    static List<ResolveChecker> resolveCheckers = Arrays.asList(
            new DDMRResolveChecker(),
            new IICVResolveChecker(),
            new IUCVResolveChecker(),
            new UDTMMRResolveChecker(),
            new UUCVResolveChecker(),
            new UUTMResolveChecker()
    );

    public ConflictResolveChecker(Client primaryClient, Client secondaryClient,
                                  String primaryvoltdbroot, String secondaryvoltdbroot) {
        this.primaryClient = primaryClient;
        this.secondaryClient = secondaryClient;
        this.primaryvoltdbroot = primaryvoltdbroot;
        this.secondaryvoltdbroot = secondaryvoltdbroot;
    }

    public void runResolveVerification() throws Exception {
        runPrimaryResolveVerification();
        runSecondaryResolveVerification();
    }

    private void runPrimaryResolveVerification() throws Exception {
        primaryClient.drain();
        primaryClient.callProcedure("@Quiesce");

        CsvConflictLoader csvLoader = new CsvConflictLoader(primaryClient, primaryvoltdbroot);
        csvLoader.load();

        LOG.info("Verifying primary cluster's XDCR conflicts");

        for (String tableName : TABLES_TO_CHECK) {
            verifyTable(primaryClient, tableName);
        }
    }

    private void runSecondaryResolveVerification() throws Exception {
        secondaryClient.drain();
        secondaryClient.callProcedure("@Quiesce");

        CsvConflictLoader csvLoader = new CsvConflictLoader(secondaryClient, secondaryvoltdbroot);
        csvLoader.load();

        LOG.info("Verifying secondary cluster's XDCR conflicts");

        for (String tableName : TABLES_TO_CHECK) {
            verifyTable(secondaryClient, tableName);
        }
    }

    private void verifyTable(Client client, String tableName) throws IOException, ProcCallException, JSONException {
        final String sqlActual = "SELECT * FROM " + tableName + "_conflict_actual WHERE cid=? AND (rid=? OR rid=?);";
        final String sqlExpected = "SELECT * FROM " + tableName + "_conflict_expected;";

        VoltTable expected = client.callProcedure("@AdHoc", sqlExpected).getResults()[0];
        while (expected.advanceRow()) {
            XdcrConflict xdcrExpected = new XdcrConflict();
            xdcrExpected.setCid((byte)expected.getLong("cid"));
            xdcrExpected.setRid(expected.getLong("rid"));
            xdcrExpected.setClusterId((int)expected.getLong("clusterid"));
            xdcrExpected.setExtRid(expected.getLong("extrid"));
            xdcrExpected.setTableName(tableName);
            xdcrExpected.setActionType(expected.getString("action_type"));
            xdcrExpected.setConflictType(expected.getString("conflict_type"));
            xdcrExpected.setDecision(expected.getString("decision"));
            xdcrExpected.setDivergenceType(expected.getString("divergence"));
            xdcrExpected.setTimeStamp(expected.getString("ts"));
            xdcrExpected.setKey(expected.getVarbinary("key"));
            xdcrExpected.setValue(expected.getVarbinary("value"));

            VoltTable actual = client.callProcedure("@AdHoc", sqlActual,
                    xdcrExpected.getCid(), xdcrExpected.getRid(), xdcrExpected.getExtRid()).getResults()[0];

            List<XdcrConflict> xdcrActuals = new ArrayList<>();
            while (actual.advanceRow()) {
                XdcrConflict xdcrActual = new XdcrConflict();
                xdcrActual.setCid((byte) actual.getLong("cid"));
                xdcrActual.setRid(actual.getLong("rid"));
                xdcrActual.setClusterId((int) actual.getLong("clusterid"));
                xdcrActual.setTableName(tableName);
                xdcrActual.setActionType(actual.getString("action_type"));
                xdcrActual.setConflictType(actual.getString("conflict_type"));
                xdcrActual.setDecision(actual.getString("decision"));
                xdcrActual.setDivergenceType(actual.getString("divergence"));
                xdcrActual.setTimeStamp(actual.getString("ts"));
                xdcrActual.setCurrentClusterId((int) actual.getLong("current_clusterid"));
                xdcrActual.setCurrentTimestamp(actual.getString("current_ts"));
                xdcrActual.setRowType(actual.getString("row_type"));
                byte[] tuple = actual.getVarbinary("tuple");
                xdcrActual.setTuple(tuple != null ? new JSONObject(new String(tuple)) : null);
                xdcrActuals.add(xdcrActual);
            }

            verifyExpectation(xdcrExpected, xdcrActuals);
        }
    }

    private void verifyExpectation(XdcrConflict xdcrExpected, List<XdcrConflict> xdcrActuals) throws JSONException {
        for (ResolveChecker checker : resolveCheckers) {
            if (checker.verifyExpectation(xdcrExpected,xdcrActuals)) {
                return;
            }
        }

        failStop("Fail to verify conflict: " + xdcrExpected);
    }

    private void waitForCsvConflictReport(Client client) throws InterruptedException, IOException, ProcCallException {
        final String applyBinaryLogSPProcName = "ApplyBinaryLogSP";
        final String applyBinaryLogMPProcName = "ApplyBinaryLogMP";

        boolean done = false;
        final long start = System.currentTimeMillis();
        long preApplyBinarySPCalls = getProcCallCounts(client, applyBinaryLogSPProcName);
        long preApplyBinaryMPCalls = getProcCallCounts(client, applyBinaryLogMPProcName);
        while (!done && System.currentTimeMillis() - start < 60000) {
            TimeUnit.SECONDS.sleep(2);
            long applyBinarySPCalls = getProcCallCounts(client, applyBinaryLogSPProcName);
            long applyBinaryMPCalls = getProcCallCounts(client, applyBinaryLogMPProcName);
            done = (preApplyBinarySPCalls == applyBinarySPCalls && preApplyBinaryMPCalls == applyBinaryMPCalls);
            preApplyBinarySPCalls = applyBinarySPCalls;
            preApplyBinaryMPCalls = applyBinaryMPCalls;
        }
    }

    static long getProcCallCounts(Client client, String applyBinaryCall) throws IOException, ProcCallException {
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

    static byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    static void checkEquals(String msgfmt, ACTION_TYPE expected, ACTION_TYPE actual) {
        if (! expected.equals(actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkEquals(String msgfmt, CONFLICT_TYPE expected, CONFLICT_TYPE actual) {
        if (! expected.equals(actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkEquals(String msgfmt, DECISION expected, DECISION actual) {
        if (! expected.equals(actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkEquals(String msgfmt, long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkEquals(String msgfmt, String expected, String actual) {
        if (! expected.equalsIgnoreCase(actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkEquals(String msgfmt, byte[] expected, byte[] actual) {
        if (! Arrays.equals(expected, actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkLessThan(String msgfmt, String existing, String expected) {
        if (existing.compareTo(expected) > 0) {
            throw new AssertionError(String.format(msgfmt, existing, expected));
        }
    }

    static void checkNotEquals(String msgfmt, String expected, String actual) {
        if (expected.equalsIgnoreCase(actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void checkNotEquals(String msgfmt, byte[] expected, byte[] actual) {
        if (Arrays.equals(expected, actual)) {
            throw new AssertionError(String.format(msgfmt, expected, actual));
        }
    }

    static void failStop(String msg) {
        throw new AssertionError(msg);
    }

    static void logXdcrVerification(String fmt, Object... args) {
        String msg = String.format(fmt, args);
        LOG.info(msg);
    }

    public static void main(String[] args) throws Exception {

        ClientConfig clientConfig = new ClientConfig("", "");
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost", 21212);
        //client.createConnection("localhost", 21214);

        String primaryRoot = "/Users/dsun/code/voltdb/tests/test_apps/xdcr-selfcheck/voltxdcr1";
        ConflictResolveChecker checker = new ConflictResolveChecker(client, null, primaryRoot, null);
        /*checker.verifyTable(client, "xdcr_replicated");
        checker.verifyTable(client, "xdcr_partitioned");*/
        checker.runPrimaryResolveVerification();
    }

}
