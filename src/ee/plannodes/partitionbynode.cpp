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
/*
 * partitionbynode.cpp
 */
#include <sstream>
#include "partitionbynode.h"
#include "common/SerializableEEException.h"

namespace voltdb {
PartitionByPlanNode::~PartitionByPlanNode()
{

}

PlanNodeType PartitionByPlanNode::getPlanNodeType() const
{
    return PLAN_NODE_TYPE_PARTITIONBY;
}

void PartitionByPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    PlannerDomValue partitionExprs = obj.valueForKey("PARTITION_BY_EXPRESSIONS");
    for (int idx = 0; idx < partitionExprs.arrayLen(); idx += 1) {
        PlannerDomValue exprDom = partitionExprs.valueAtIndex(idx);
        m_partitionExpressions.push_back(AbstractExpression::buildExpressionTree(exprDom));
    }
    std::string valStr;
    PlannerDomValue aggOpObj = obj.valueForKey("AGGREGATE_OPERATION");
    valStr = aggOpObj.asStr();
    m_aggregateOperation = stringToExpression(valStr);
    PlannerDomValue aggValObj = obj.valueForKey("AGGREGATE_VALUE_TYPE");
    valStr = aggValObj.asStr();
    m_valueType = stringToValue(valStr);
    PlannerDomValue aggValSizeObj = obj.valueForKey("AGGREGATE_VALUE_SIZE");
    m_valueSize = aggValSizeObj.asInt();
}

std::string PartitionByPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << "PartitionByPlanNode: ";
    for (int idx = 0; idx < m_partitionExpressions.size(); idx += 1) {
        buffer << m_partitionExpressions[idx]->debug(spacer);
    }
    return buffer.str();
}
}
