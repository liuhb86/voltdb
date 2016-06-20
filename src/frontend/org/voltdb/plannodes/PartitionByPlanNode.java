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
package org.voltdb.plannodes;

import java.util.ArrayList;
import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowedExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * This plan node represents windowed aggregate computations.
 * The only one we implement now is windowed RANK.  But more
 * could be possible.
 */
public class PartitionByPlanNode extends AbstractPlanNode {
    public enum Members {
        AGGREGATE_OPERATION,
        AGGREGATE_COLUMN_NAME,
        AGGREGATE_COLUMN_ALIAS,
        AGGREGATE_TABLE_NAME,
        AGGREGATE_TABLE_ALIAS,
        AGGREGATE_VALUE_TYPE,
        AGGREGATE_VALUE_SIZE,
        PARTITION_BY_EXPRESSIONS
    };

    public PartitionByPlanNode() {
        m_outputSchema = new NodeSchema();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTITIONBY;
    }

    @Override
    public void resolveColumnIndexes() {
        /*
         * We need to resolve all the tves in the partition by and
         * order by expressions of the windowed expression.
         */
        assert (m_children.size() == 1);
        m_children.get(0).resolveColumnIndexes();
        NodeSchema input_schema = m_children.get(0).getOutputSchema();

        List<AbstractExpression> tves = getAllTVEs();
        // Resolve all of them.
        for (AbstractExpression ae : tves) {
            TupleValueExpression tve = (TupleValueExpression)ae;
            int index = tve.resolveColumnIndexesUsingSchema(input_schema);
            if (index == -1) {
                // check to see if this TVE is the aggregate output
                // XXX SHOULD MODE THIS STRING TO A STATIC DEF SOMEWHERE
                if (!tve.getTableName().equals("VOLT_TEMP_TABLE")) {
                    throw new RuntimeException("Unable to find index for column: " +
                                               tve.getColumnName());
                }
            } else {
                tve.setColumnIndex(index);
            }
        }
    }

    /**
     * This is used to get all the TVEs in the partition by plan node.  It's
     * only used for resolution of TVE indices.  But it may be useful in
     * testing that the TVEs are properly resolved,
     *
     * @return All the TVEs in expressions in this plan node.
     */
    public List<AbstractExpression> getAllTVEs() {
        List<AbstractExpression> tves =  new ArrayList<AbstractExpression>();
        // Get everything in the out columns but the aggregate column.  The aggregate
        // column's column index is not helpful.
        List<SchemaColumn> outputColumns = getOutputSchema().getColumns();
        for (int idx = 1; idx < outputColumns.size(); idx += 1) {
            SchemaColumn col = outputColumns.get(idx);
            AbstractExpression expr = col.getExpression();
            if (col != null) {
                tves.addAll(expr.findAllSubexpressionsOfClass(TupleValueExpression.class));
            }
        }
        return tves;
    }

    /**
     * Generate the output schema.  We need to create the aggregate column,
     * and then we need to copy the columns of the input schema to the output schema.
     */
    @Override
    public void generateOutputSchema(Database db) {
        assert(getChildCount() == 1);

        // Do the children's generation.
        m_children.get(0).generateOutputSchema(db);

        // Now, generate the output columns for this plan node.  First
        // add a column for the windowed output.  Then add the input columns.
        assert(m_outputSchema != null);
        assert(0 == m_outputSchema.getColumns().size());
        // We don't serialize the column and table names and aliases.
        // The column indices are 0 for the aggregate column, but they
        // don't actually matter, because we are going to compute it.
        TupleValueExpression tve = new TupleValueExpression();
        tve.setValueSize(m_aggregateValueSize);
        tve.setValueType(m_aggregateValueType);
        // This is generated, so it has no column index in the output schema.
        tve.setColumnIndex(0);
        SchemaColumn col = new SchemaColumn(m_aggregateTableName,
                                            m_aggregateTableAlias,
                                            m_aggregateTableName,
                                            m_aggregateColumnAlias,
                                            tve);
        m_outputSchema.addColumn(col);
        m_hasSignificantOutputSchema = true;
        NodeSchema inputSchema = getChild(0).getOutputSchema();
        assert(inputSchema != null);
        for (SchemaColumn schemaCol : inputSchema.getColumns()) {
            // We have to clone this because we will be
            // horsing around with the indices of TVEs.  However,
            // we don't need to resolve anything here, because
            // the default column index algorithm will work quite
            // nicely for us.
            SchemaColumn newCol = schemaCol.clone();
            m_outputSchema.addColumn(newCol);
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String optionalTableName = "*NO MATCH -- USE ALL TABLE NAMES*";
        String newIndent = "  " + indent;
        StringBuilder sb = new StringBuilder(indent + "PARTITION BY PLAN\n");
        sb.append(newIndent + "PARTITION BY:\n");
        int numExprs = getNumberOfPartitionByExpressions();
        for (int idx = 0; idx < numExprs; idx += 1) {
            AbstractExpression ae = getPartitionByExpression(idx);
            // Apparently ae.toString() adds a trailing newline.  That's
            // unfortunate, but it works out ok here.
            sb.append("  ")
              .append(newIndent)
              .append(idx).append(": ")
              .append(ae.toString());
        }
        String sep = "";
        sb.append(newIndent).append("SORT BY:\n");
        numExprs = numberSortExpressions();
        for (int idx = 0; idx < numExprs; idx += 1) {
            AbstractExpression ae = getSortExpression(idx);
            SortDirectionType dir = getSortDirection(idx);
            sb.append(sep).append("  ")
              .append(newIndent)
              .append(idx).append(":")
              .append(ae.explain(optionalTableName))
              .append(" ")
              .append(dir.name());
            sep = "\n";
        }
        return sb.toString();
    }

    private void saveStringAsJSON(JSONStringer stringer, String name, String key) throws JSONException {
        if ((name != null) && (name.length() > 0)) {
            stringer.key(key).value(name);
        }
    }

    private void saveIntAsJSON(JSONStringer stringer, int value, String key) throws JSONException {
        stringer.key(key).value(value);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.AGGREGATE_OPERATION.name())
                .value(m_aggregateOperation);
        stringer.key(Members.PARTITION_BY_EXPRESSIONS.name());
        stringer.array();
        for (AbstractExpression expr : m_partitionByExpressions) {
            stringer.object();
            expr.toJSONString(stringer);
            stringer.endObject();
        }
        stringer.endArray();
        // Save the information associated with the windowed column.
        saveStringAsJSON(stringer, m_aggregateTableName,   Members.AGGREGATE_TABLE_NAME.name());
        saveStringAsJSON(stringer, m_aggregateTableAlias,  Members.AGGREGATE_TABLE_ALIAS.name());
        saveStringAsJSON(stringer, m_aggregateColumnName,  Members.AGGREGATE_COLUMN_NAME.name());
        saveStringAsJSON(stringer, m_aggregateColumnAlias, Members.AGGREGATE_COLUMN_ALIAS.name());
        saveIntAsJSON   (stringer, m_aggregateValueType.getValue(),
                                                           Members.AGGREGATE_VALUE_TYPE.name());
        saveIntAsJSON   (stringer, m_aggregateValueSize,   Members.AGGREGATE_VALUE_SIZE.name());
    }

    private String chooseString(JSONObject jobj, String key) throws JSONException {
        if (jobj.has(key)) {
            return jobj.getString(key);
        }
        return null;
    }

    private VoltType chooseType(JSONObject jobj, String key) throws JSONException {
        if (jobj.has(key)) {
            int val = jobj.getInt(key);
            return VoltType.get((byte)val);
        }
        return null;
    }

    private int chooseInt(JSONObject jobj, String key) throws JSONException {
        if (jobj.has(key)) {
            return jobj.getInt(key);
        }
        return -1;
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        int aggop = jobj.getInt(Members.AGGREGATE_OPERATION.name());
        m_aggregateOperation = ExpressionType.get(aggop);
        AbstractExpression.loadFromJSONArrayChild(m_partitionByExpressions,
                                                  jobj,
                                                  Members.PARTITION_BY_EXPRESSIONS.name(),
                                                  null);
        // Read the windowed column metadata.
        m_aggregateTableName       = chooseString(jobj, Members.AGGREGATE_TABLE_NAME.name());
        m_aggregateTableAlias      = chooseString(jobj, Members.AGGREGATE_TABLE_ALIAS.name());
        m_aggregateColumnName      = chooseString(jobj, Members.AGGREGATE_COLUMN_NAME.name());
        m_aggregateColumnAlias     = chooseString(jobj, Members.AGGREGATE_COLUMN_ALIAS.name());
        m_aggregateValueType       = chooseType  (jobj, Members.AGGREGATE_VALUE_TYPE.name());
        m_aggregateValueSize       = chooseInt   (jobj, Members.AGGREGATE_VALUE_SIZE.name());
        if (jobj.has(Members.AGGREGATE_VALUE_TYPE.name())) {
            int valType = jobj.getInt(Members.AGGREGATE_VALUE_TYPE.name());
            m_aggregateValueType = VoltType.get((byte)valType);
        }
        if (jobj.has(Members.AGGREGATE_VALUE_SIZE.name())) {
            m_aggregateValueSize = jobj.getInt(Members.AGGREGATE_VALUE_SIZE.name());
        }
        assert(0 <= m_aggregateValueSize);
        assert(m_aggregateValueType != null);
        assert(m_aggregateTableAlias != null || m_aggregateTableName != null);
        assert(m_aggregateColumnAlias != null || m_aggregateColumnName != null);
    }

    public AbstractExpression getPartitionByExpression(int idx) {
        return m_partitionByExpressions.get(idx);
    }

    public int getNumberOfPartitionByExpressions() {
        return m_partitionByExpressions.size();
    }

    public AbstractExpression getSortExpression(int idx) {
        return m_orderByExpressions.get(idx);
    }

    public SortDirectionType getSortDirection(int idx) {
        return m_orderBySortDirections.get(idx);
    }

    public int numberSortExpressions() {
        return m_orderByExpressions.size();
    }

    public void setWindowedColumn(SchemaColumn col) {
        WindowedExpression winex = (WindowedExpression)col.getExpression();
        m_aggregateTableName = col.getTableName();
        m_aggregateTableAlias = col.getTableAlias();
        m_aggregateColumnName = col.getColumnName();
        m_aggregateColumnAlias = col.getColumnAlias();
        m_aggregateOperation = winex.getExpressionType();
        m_partitionByExpressions = winex.getPartitionByExpressions();
        m_orderByExpressions = winex.getOrderByExpressions();
        m_orderBySortDirections = winex.getOrderByDirections();
        m_aggregateValueType = winex.getValueType();
        m_aggregateValueSize = winex.getValueSize();
    }

    @Override
    /**
     * This node needs a projection node.
     */
    public boolean planNodeClassNeedsProjectionNode() {
        return true;
    }

    private ExpressionType           m_aggregateOperation     = null;
    private List<AbstractExpression> m_partitionByExpressions = new ArrayList<>();
    private List<AbstractExpression> m_orderByExpressions     = new ArrayList<>();
    private List<SortDirectionType>  m_orderBySortDirections  = new ArrayList<>();
    private String                   m_aggregateTableName;
    private String                   m_aggregateTableAlias;
    private String                   m_aggregateColumnName;
    private String                   m_aggregateColumnAlias;
    private VoltType                 m_aggregateValueType;
    private int                      m_aggregateValueSize;
}
