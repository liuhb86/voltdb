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

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import xdcrSelfCheck.ClientPayloadProcessor;
import xdcrSelfCheck.ClientThread;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class DDMRConflictRunner extends ClientConflictRunner {

    DDMRConflictRunner(ClientThread clientThread) {
        super(clientThread);
    }

    @Override
    public void runScenario(final String tableName) throws Throwable {
        final String insertProcName = getInsertProcCall(tableName);
        final ClientPayloadProcessor.Pair seedPayload = processor.generateForStore();

        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(primaryClient, rid, insertProcName, seedPayload);
                            waitForApplyBinaryLog(cid, tableName, primaryClient, 1, secondaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            primary.join();

            primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = primaryClient.callProcedure(tableName.toUpperCase() + ".delete", cid, rid).getResults();
                            waitForApplyBinaryLog(cid, tableName, primaryClient, 0, secondaryClient, 0);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return secondaryClient.callProcedure(tableName.toUpperCase() + ".delete", cid, rid).getResults();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            CompletableFuture.allOf(primary, secondary).join();

            final String readProcName = getReadProcCall(tableName);
            VoltTable primaryData = primaryClient.callProcedure(readProcName, cid).getResults()[0];
            if (primaryData.getRowCount() != 0) {
                throw new VoltAbortException(
                        "The primary partitioned table has more rows than expected: actual " + primaryData.getRowCount() + ", expected 0");
            }

            VoltTable secondaryData = secondaryClient.callProcedure(readProcName, cid).getResults()[0];
            if (secondaryData.getRowCount() != 0) {
                throw new VoltAbortException(
                        "The secondary partitioned table has more rows than expected: actual " + secondaryData.getRowCount() + ", expected 0");
            }

        } catch (CompletionException ce) {
            throw ce.getCause();
        } catch (Exception e) {
            throw e;
        } finally {
            resetTable(tableName);
        }
    }
}
