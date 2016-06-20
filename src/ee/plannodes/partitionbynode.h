/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#define SRC_EE_PLANNODES_PARTITIONBYNODE_H_
#include "abstractplannode.h"

namespace voltdb {
/**
 * In the EE, a PartitionByPlanNode is considerably simpler than the
 * Java version, since we don't have to serialize it.  We just have
 * to deserialize them.  So we don't need to remember the table and
 * column names and aliases.
 */
class PartitionByPlanNode : public AbstractPlanNode {
public:
    PartitionByPlanNode()
        : m_aggregateOperation(EXPRESSION_TYPE_INVALID) {
    }
    ~PartitionByPlanNode();

    PlanNodeType getPlanNodeType() const;
    std::string debugInfo(const std::string &spacer) const;

    const std::vector<AbstractExpression*> getPartitionExpressions() const {
        return m_partitionExpressions;
    }

    ExpressionType getAggregateOperation() const {
        return m_aggregateOperation;
    }

    ValueType getValueType() const {
        return m_valueType;
    }

    int getValueSize() const {
        return m_valueSize;
    }

protected:
    void loadFromJSONObject(PlannerDomValue obj);

private:
    OwningExpressionVector          m_partitionExpressions;
    ExpressionType                  m_aggregateOperation;
    ValueType                       m_valueType;
    int                             m_valueSize;
};
}
#endif /* SRC_EE_PLANNODES_PARTITIONBYNODE_H_ */
