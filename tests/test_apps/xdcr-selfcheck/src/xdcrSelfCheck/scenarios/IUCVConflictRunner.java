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

class IUCVConflictRunner extends ClientConflictRunner {

    IUCVConflictRunner(ClientThread clientThread) {
        super(clientThread);
    }

    @Override
    public void runScenario(final String tableName) throws Throwable {
        try {
            final String insertProcName = getInsertProcCall(tableName);
            final ClientPayloadProcessor.Pair primaryPayload = processor.generateForStore();
            final ClientPayloadProcessor.Pair secondaryPayload = processor.generateForStore();

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(secondaryClient, rid, insertProcName, secondaryPayload);
                            waitForApplyBinaryLog(cid, tableName, secondaryClient, 1, primaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            secondary.join();

            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(primaryClient, rid + 1, insertProcName, primaryPayload);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            final String updateProcName = getUpdateProcCall(tableName);
            secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(secondaryClient, rid, updateProcName, primaryPayload);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            try {
                CompletableFuture.allOf(primary, secondary).join();
                VoltTable[] primaryResult = primary.get();
                VoltTable primaryData = primaryResult[2];
                verifyTableData("primary", tableName, rid + 1, primaryData, 2, 0, primaryPayload);

                VoltTable[] secondaryResult = secondary.get();
                VoltTable secondaryData = secondaryResult[1];
                verifyTableData("secondary", tableName, rid, secondaryData, 1, 0, primaryPayload);
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
