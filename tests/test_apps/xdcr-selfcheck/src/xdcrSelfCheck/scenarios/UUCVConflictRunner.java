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

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import xdcrSelfCheck.ClientPayloadProcessor;
import xdcrSelfCheck.ClientThread;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class UUCVConflictRunner extends ClientConflictRunner {

    UUCVConflictRunner(ClientThread clientThread) {
        super(clientThread);
    }

    @Override
    public void runScenario(final String tableName) throws Throwable {
        final String insertProcName = getInsertProcCall(tableName);
        final ClientPayloadProcessor.Pair primaryPayload = processor.generateForStore();
        final ClientPayloadProcessor.Pair secondaryPayload = processor.generateForStore();
        final ClientPayloadProcessor.Pair sharedPayload = processor.generateForStore();

        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(primaryClient, rid, insertProcName, primaryPayload);
                            waitForApplyBinaryLog(cid, tableName, primaryClient, 1, secondaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            primary.join();

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(secondaryClient, rid + 1, insertProcName, secondaryPayload);
                            waitForApplyBinaryLog(cid, tableName, secondaryClient, 2, primaryClient, 2);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            secondary.join();

            final String updateProcName = getUpdateProcCall(tableName);
            primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(primaryClient, rid, updateProcName, sharedPayload);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(secondaryClient, rid + 1, updateProcName, sharedPayload);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            try {
                CompletableFuture.allOf(primary, secondary).join();
                VoltTable[] primaryResult = primary.get();
                VoltTable primaryData = primaryResult[1];
                verifyTableData("primary", tableName, rid, primaryData, 2, 1, sharedPayload);

                VoltTable[] secondaryResult = secondary.get();
                VoltTable secondaryData = secondaryResult[1];
                verifyTableData("secondary", tableName, rid + 1, secondaryData, 2, 0, sharedPayload);
            } catch (CompletionException ce) {
                Throwable cause = ce.getCause();
                if (cause instanceof ProcCallException) {
                    ProcCallException pe = (ProcCallException) cause;
                    ClientResponseImpl cri = (ClientResponseImpl) pe.getClientResponse();
                    if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE)) {
                        LOG.warn("Received store procedure exceptions", pe);
                        // no xdcr conflict
                        return;
                    }
                }

                throw ce;
            }
        } catch (CompletionException ce) {
            throw ce.getCause();
        } finally {
            resetTable(tableName);
        }
    }
}
