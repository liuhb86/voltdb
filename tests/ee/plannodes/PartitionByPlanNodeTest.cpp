/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * PartitionByPlanNode.cpp
 *
 * Test the PartitionByPlanNode.  There is not much semantics here,
 * so we just test the json reading.
 */
#include "plannodes/partitionbynode.h"
#include "common/PlannerDomValue.h"
#include "expressions/tuplevalueexpression.h"

#include "harness.h"
using namespace voltdb;

namespace {
const char *jsonStrings[] = {
                "{\n"
                "    \"AGGREGATE_COLUMN_ALIAS\": \"ARANK\",\n"
                "    \"AGGREGATE_OPERATION\": \"AGGREGATE_WINDOWED_RANK\",\n"
                "    \"AGGREGATE_TABLE_ALIAS\": \"VOLT_TEMP_TABLE\",\n"
                "    \"AGGREGATE_TABLE_NAME\": \"VOLT_TEMP_TABLE\",\n"
                "    \"AGGREGATE_VALUE_SIZE\": 8,\n"
                "    \"AGGREGATE_VALUE_TYPE\": 6,\n"
                "    \"CHILDREN_IDS\": [3],\n"
                "    \"ID\": 2,\n"
                "    \"OUTPUT_SCHEMA\": [\n"
                "        {\n"
                "            \"COLUMN_NAME\": \"ARANK\",\n"
                "            \"EXPRESSION\": {\n"
                "                \"COLUMN_IDX\": 0,\n"
                "                \"TYPE\": 32,\n"
                "                \"VALUE_TYPE\": 6\n"
                "            }\n"
                "        },\n"
                "        {\n"
                "            \"COLUMN_NAME\": \"A\",\n"
                "            \"EXPRESSION\": {\n"
                "                \"COLUMN_IDX\": 0,\n"
                "                \"TYPE\": 32,\n"
                "                \"VALUE_TYPE\": 5\n"
                "            }\n"
                "        },\n"
                "        {\n"
                "            \"COLUMN_NAME\": \"A\",\n"
                "            \"EXPRESSION\": {\n"
                "                \"COLUMN_IDX\": 1,\n"
                "                \"TYPE\": 32,\n"
                "                \"VALUE_TYPE\": 5\n"
                "            }\n"
                "        },\n"
                "        {\n"
                "            \"COLUMN_NAME\": \"B\",\n"
                "            \"EXPRESSION\": {\n"
                "                \"COLUMN_IDX\": 2,\n"
                "                \"TYPE\": 32,\n"
                "                \"VALUE_TYPE\": 5\n"
                "            }\n"
                "        },\n"
                "        {\n"
                "            \"COLUMN_NAME\": \"B\",\n"
                "            \"EXPRESSION\": {\n"
                "                \"COLUMN_IDX\": 3,\n"
                "                \"TYPE\": 32,\n"
                "                \"VALUE_TYPE\": 5\n"
                "            }\n"
                "        }\n"
                "    ],\n"
                "    \"PARTITION_BY_EXPRESSIONS\": [\n"
                "        {\n"
                "            \"COLUMN_IDX\": 0,\n"
                "            \"TYPE\": 32,\n"
                "            \"VALUE_TYPE\": 5\n"
                "        },\n"
                "        {\n"
                "            \"COLUMN_IDX\": 1,\n"
                "            \"TYPE\": 32,\n"
                "            \"VALUE_TYPE\": 5\n"
                "        }\n"
                "    ],\n"
                "    \"PLAN_NODE_TYPE\": \"PARTITIONBY\"\n"
                "}\n",
                (const char *)0
};

class PartitionByPlanNodeTest : public Test {
public:
    PartitionByPlanNodeTest()
    {
    }
};

TEST_F(PartitionByPlanNodeTest, TestJSON)
{
    for (int jsonIdx = 0; jsonStrings[jsonIdx]; jsonIdx += 1) {
        const char *jsonString = jsonStrings[jsonIdx];
        PlannerDomRoot root(jsonString);
        if (root.isNull()) {
            EXPECT_TRUE(false);
            return;
        }
        PlannerDomValue obj(root.rootObject());
        boost::shared_ptr<voltdb::PartitionByPlanNode> pn(dynamic_cast<PartitionByPlanNode*>(AbstractPlanNode::fromJSONObject(obj)));
        EXPECT_NE(NULL, pn.get());
        const std::vector<AbstractExpression*> &partitionByExprs = pn->getPartitionExpressions();
        EXPECT_EQ(2, partitionByExprs.size());
        for (int exprIdx = 0; exprIdx < partitionByExprs.size(); exprIdx += 1) {
            TupleValueExpression *tve = dynamic_cast<TupleValueExpression*>(partitionByExprs[exprIdx]);
            EXPECT_NE(NULL, tve);
            // These three are all true because of collusion in the
            // construction of the JSON.
            EXPECT_EQ(exprIdx + 1,              tve->getColumnId());
            EXPECT_EQ(((exprIdx == 0) ? 8 : 5), tve->getValueType());
            EXPECT_EQ(((exprIdx == 0) ? 8 : 4), tve->getValueSize());
        }
        EXPECT_EQ(EXPRESSION_TYPE_AGGREGATE_WINDOWED_RANK, pn->getAggregateOperation());
        EXPECT_EQ(VALUE_TYPE_BIGINT, pn->getValueType());
        EXPECT_EQ(8, pn->getValueSize());
    }
}
}


int main()
{
    return TestSuite::globalInstance()->runAll();
}
