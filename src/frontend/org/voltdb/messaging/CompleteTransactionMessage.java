/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;

public class CompleteTransactionMessage extends TransactionInfoBaseMessage
{
    boolean m_isRollback;
    boolean m_requiresAck;
    boolean m_rollbackForFault;

    int m_flags = 0;
    static final int ISROLLBACK = 0;
    static final int REQUIRESACK = 1;
    static final int ROLLBACKFORFAULT = 2;

    private void setBit(int position, boolean value)
    {
        if (value) {
            m_flags |= (1 << position);
        }
        else {
            m_flags &= ~(1 << position);
        }
    }

    private boolean getBit(int position)
    {
        return (((m_flags >> position) & 0x1) == 1);
    }

    /** Empty constructor for de-serialization */
    CompleteTransactionMessage() {
        super();
    }

    /**
     * These four args needed for base class
     * @param initiatorHSId
     * @param coordinatorHSId
     * @param txnId
     * @param isReadOnly
     *
     * @param isRollback  Should the recipient rollback this transaction to complete it?
     * @param requiresAck  Does the recipient need to respond to this message
     *                     with a CompleteTransactionResponseMessage?
     */
    public CompleteTransactionMessage(long initiatorHSId, long coordinatorHSId,
                                      long txnId, boolean isReadOnly,
                                      boolean isRollback, boolean requiresAck,
                                      boolean rollbackForFault, boolean isForReplay)
    {
        super(initiatorHSId, coordinatorHSId, txnId, 0, isReadOnly, isForReplay);
        setBit(ISROLLBACK, isRollback);
        setBit(REQUIRESACK, requiresAck);
        setBit(ROLLBACKFORFAULT, rollbackForFault);
    }

    public boolean isRollback()
    {
        return getBit(ISROLLBACK);
    }

    public boolean requiresAck()
    {
        return getBit(REQUIRESACK);
    }

    public boolean isRollbackForFault()
    {
        return getBit(ROLLBACKFORFAULT);
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.COMPLETE_TRANSACTION_ID);
        super.flattenToBuffer(buf);
        buf.putInt(m_flags);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);
        m_flags = buf.getInt();
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION (FROM COORD: ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append("\n  FLAGS: ").append(m_flags);

        if (isRollback())
            sb.append("\n  THIS IS AN ROLLBACK REQUEST");

        if (requiresAck())
            sb.append("\n  THIS MESSAGE REQUIRES AN ACK");

        if (isRollbackForFault()) {
            sb.append("\n  THIS ROLLBACK IS FOR FAULT REPAIR");
        }

        return sb.toString();
    }
}
