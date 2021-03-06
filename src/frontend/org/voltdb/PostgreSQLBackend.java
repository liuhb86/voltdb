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

package org.voltdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

/**
 * A wrapper around a PostgreSQL database server, and JDBC connection. This
 * class can be used to execute SQL statements instead of the C++ ExecutionEngine.
 * It is currently used only by the SQL Coverage tests (and perhaps, someday,
 * the JUnit regressionsuite tests).
 */
public class PostgreSQLBackend extends NonVoltDBBackend {
    /** java.util.logging logger. */
    @SuppressWarnings("unused")
    private static final VoltLogger log = new VoltLogger(PostgreSQLBackend.class.getName());

    protected static final String m_default_username = "postgres";
    protected static final String m_default_password = "voltdb";
    protected static final String m_permanent_database_name = "postgres";
    protected static final String m_database_name = "sqlcoveragetest";
    protected static PostgreSQLBackend m_permanent_db_backend = null;
    // PostgreSQL column type names that are not found in VoltDB or HSQL,
    // mapped to their VoltDB/HSQL equivalents
    private static final Map<String,String> m_PostgreSQLTypeNames;
    static {
        m_PostgreSQLTypeNames = new HashMap<String,String>();
        m_PostgreSQLTypeNames.put("int2", "SMALLINT");
        m_PostgreSQLTypeNames.put("int4", "INTEGER");
        m_PostgreSQLTypeNames.put("int8", "BIGINT");
        m_PostgreSQLTypeNames.put("float8", "FLOAT");
        m_PostgreSQLTypeNames.put("numeric", "DECIMAL");
        m_PostgreSQLTypeNames.put("bytea", "VARBINARY");
        m_PostgreSQLTypeNames.put("varbit", "VARBINARY");
        m_PostgreSQLTypeNames.put("char", "CHARACTER");
        m_PostgreSQLTypeNames.put("text", "VARCHAR");
        m_PostgreSQLTypeNames.put("geography", "GEOGRAPHY");
        // NOTE: what VoltDB calls "GEOGRAPHY_POINT" would also be called
        // "geography" by PostgreSQL, so this mapping is imperfect; however,
        // so far this has not been a problem
    }

    // Captures the use of ORDER BY, with up to 6 order-by columns; beyond
    // those will be ignored (similar to
    // voltdb/tests/scripts/examples/sql_coverage/StandardNormalzer.py)
    private static final Pattern orderByQuery = Pattern.compile(
            "ORDER BY(?<column1>\\s+(\\w*\\s*\\(\\s*)*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?)"
            + "((?<column2>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column3>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column4>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column5>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?"
            + "((?<column6>\\s*,\\s*(\\w*\\s*\\()*\\s*(\\w+\\.)?\\w+((\\s+(AS|FROM)\\s+\\w+)?\\s*\\))*(\\s+(ASC|DESC))?))?",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an ORDER BY clause, by adding (for each
    // order-by column) either NULLS FIRST or (after "DESC") NULL LAST, so
    // that PostgreSQL results will match VoltDB results
    private static final QueryTransformer orderByQueryTransformer
            = new QueryTransformer(orderByQuery)
            .initialText("ORDER BY").suffix(" NULLS FIRST")
            .alternateSuffix("DESC", " NULLS LAST")
            .groups("column1", "column2", "column3", "column4", "column5", "column6");

    // Captures the use of EXTRACT(DAY_OF_WEEK FROM ...)
    private static final Pattern dayOfWeekQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_WEEK\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_WEEK FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOW FROM ...)+1, which is an equivalent that PostgreSQL does
    // support. (The '+1' is because PostgreSQL counts Sunday as 0 and Saturday
    // as 6, etc., whereas VoltDB counts Sunday as 1 and Saturday as 7, etc.)
    private static final QueryTransformer dayOfWeekQueryTransformer
            = new QueryTransformer(dayOfWeekQuery)
            .initialText("EXTRACT ( ").prefix("DOW FROM").suffix(")+1").groups("column");

    // Captures the use of EXTRACT(DAY_OF_YEAR FROM ...)
    private static final Pattern dayOfYearQuery = Pattern.compile(
            "EXTRACT\\s*\\(\\s*DAY_OF_YEAR\\s+FROM(?<column>\\s+\\w+\\s*)\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an EXTRACT(DAY_OF_YEAR FROM ...)
    // function, which PostgreSQL does not support, and replaces it with
    // EXTRACT(DOY FROM ...), which is an equivalent that PostgreSQL does
    // support
    private static final QueryTransformer dayOfYearQueryTransformer
            = new QueryTransformer(dayOfYearQuery)
            .initialText("EXTRACT ( ").prefix("DOY FROM").suffix(")").groups("column");

    // Captures the use of AVG(columnName), which PostgreSQL handles
    // differently, when the columnName is of one of the integer types
    private static final Pattern avgQuery = Pattern.compile(
            "AVG\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing an AVG(columnName) function, where
    // <i>columnName</i> is of an integer type, for which PostgreSQL returns
    // a numeric (non-integer) value, unlike VoltDB, which returns an integer;
    // so change it to: TRUNC ( AVG(columnName) )
    private static final QueryTransformer avgQueryTransformer
            = new QueryTransformer(avgQuery)
            .prefix("TRUNC ( ").suffix(" )").groups("column")
            .useWholeMatch().columnType(ColumnType.INTEGER);

    // Captures the use of CEILING(columnName) or FLOOR(columnName)
    private static final Pattern ceilingOrFloorQuery = Pattern.compile(
            "(CEILING|FLOOR)\\s*\\((\\s*\\w*\\s*\\()*\\s*(\\w+\\.)?(?<column>\\w+)(\\s*\\)(\\s+(AS|FROM)\\s+\\w+)?)*\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a CEILING(columnName) or FLOOR(columnName)
    // function, where <i>columnName</i> is of an integer type, for which
    // PostgreSQL returns a numeric (non-integer) value, unlike VoltDB, which
    // returns an integer; so change it to:
    // CAST ( CEILING(columnName) as INTEGER ), or
    // CAST ( FLOOR(columnName) as INTEGER ), respectively.
    private static final QueryTransformer ceilingOrFloorQueryTransformer
            = new QueryTransformer(ceilingOrFloorQuery)
            .prefix("CAST ( ").suffix(" as INTEGER )").groups("column")
            .useWholeMatch().columnType(ColumnType.INTEGER);

    // Used in both versions, below, of an UPSERT statement: an
    // UPSERT INTO VALUES or an UPSERT INTO SELECT
    private static final String UPSERT_QUERY_START = "(?<upsert>UPSERT)\\s+INTO\\s+(?<table>\\w+)\\s+"
            + "(\\(\\s*(?<columns>\\w+\\s*(,\\s*\\w+\\s*)*)\\)\\s+)?";
    // Used below (twice), for an UPSERT INTO SELECT statement
    private static final String SORT_KEYWORDS = "GROUP\\s+BY|HAVING|ORDER\\s+BY|LIMIT|OFFSET";

    // Captures the use of an UPSERT INTO VALUES statement, for example:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // where the column list, here "(C1, C2, C3)", is optional; the values
    // list, here "(1, 'abc', 12.34)", can include arbitrary values; and both
    // can include any number of items. (Though, for a valid UPSERT, the number
    // of values must match the number of columns, when included, or else the
    // number of columns defined in table T1; and the types must also match.)
    private static final Pattern upsertValuesQuery = Pattern.compile(
            UPSERT_QUERY_START + "VALUES\\s+\\(\\s*(?<values>.+)\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies an UPSERT INTO VALUES statement, as described above, such as:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // which PostgreSQL does not support, and replaces it with an INSERT
    // statement using ON CONFLICT DO UPDATE, such as:
    //     INSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34) ON CONFLICT (C1)
    //         DO UPDATE SET (C2, C3) = ('abc', 12.34)
    // which is an equivalent that PostgreSQL does support. (This example
    // assumes that the C1 column is the primary key.)
    private static final QueryTransformer upsertValuesQueryTransformer
            = new QueryTransformer(upsertValuesQuery)
            .groups("upsert", "table", "columns", "values")
            .groupReplacementText("INSERT").useWholeMatch()
            .suffix(" ON CONFLICT ({columns:pk}) DO UPDATE SET ({columns:npk}) = ({values:npk})");

    // Captures the use of an UPSERT INTO VALUES statement, for example:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // where the column list, here "(C1, C2, C3)", is optional; the values
    // list, here "(1, 'abc', 12.34)", can include arbitrary values; and both
    // can include any number of items. (Though, for a valid UPSERT, the number
    // of values must match the number of columns, when included, or else the
    // number of columns defined in table T1; and the types must also match.)
    private static final Pattern upsertSelectQuery = Pattern.compile(
            UPSERT_QUERY_START + "SELECT\\s+(?<values>[+\\-*\\/%|'\\s\\w]+(,\\s*[+\\-*\\/%|'\\s\\w]+)*)\\s+"
                    + "FROM\\s+(?<selecttables>\\w+(\\s+AS\\s+\\w+)?((\\s*,\\s*|\\s+JOIN\\s+)\\w+(\\s+AS\\s+\\w+)?)*)\\s+"
                    + "(?<where>WHERE\\s+((?!"+SORT_KEYWORDS+").)+)?"
                    + "(?<sort>("+SORT_KEYWORDS+").+)?",
            Pattern.CASE_INSENSITIVE);
    // Modifies an UPSERT INTO VALUES statement, as described above, such as:
    //     UPSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34)
    // which PostgreSQL does not support, and replaces it with an INSERT
    // statement using ON CONFLICT DO UPDATE, such as:
    //     INSERT INTO T1 (C1, C2, C3) VALUES (1, 'abc', 12.34) ON CONFLICT (C1)
    //         DO UPDATE SET (C2, C3) = ('abc', 12.34)
    // which is an equivalent that PostgreSQL does support. (This example
    // assumes that the C1 column is the primary key.)
    private static final QueryTransformer upsertSelectQueryTransformer
            = new QueryTransformer(upsertSelectQuery)
            .groups("upsert", "table", "columns", "values", "selecttables", "where", "sort")
            .groupReplacementText("INSERT", "{table} AS _TMP").useWholeMatch()
            .suffix(" ON CONFLICT ({columns:pk}) DO UPDATE SET ({columns:npk}) = "
                    + "(SELECT {values:npk} FROM {selecttables} {where:pk} {sort})");

    // Captures the use of string concatenation using a plus sign (+), e.g.:
    //     'str' + VCHAR
    // or, in the reverse order:
    //     VCHAR + 'str'
    // Note that this would not capture the concatenation of two VARCHAR
    // columns using a plus sign, e.g. VCHAR1 + VCHAR2, but there is no simple
    // way (without querying meta-data) to distinguish that from addition of
    // two numeric columns, which we would not want to change; so far, this
    // has not been a problem.
    private static final Pattern stringConcatQuery = Pattern.compile(
            "'\\w+'\\s*(?<plus>\\+)|(?<plus2>\\+)\\s*'\\w+'",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing 'FOO' + ..., which PostgreSQL does not
    // support, and replaces it with 'FOO' || ..., which is an equivalent
    // that PostgreSQL does support
    private static final QueryTransformer stringConcatQueryTransformer
            = new QueryTransformer(stringConcatQuery)
            .replacementText("||").useWholeMatch().groups("plus", "plus2");

    // Captures the use of a VARBINARY constant, e.g. x'12AF'
    private static final Pattern varbinaryConstant = Pattern.compile(
            "x'(?<bytes>[0-9A-Fa-f]+)'",
            Pattern.CASE_INSENSITIVE);
    // Modifies a query containing a VARBINARY constant, e.g. x'12AF', which
    // PostgreSQL does not support in that format, and replaces it with a
    // VARBINARY constant in the format it does support, e.g. E'\\x12AF'
    // (with lots of extra backslashes, for escaping at various levels)
    private static final QueryTransformer varbinaryConstantTransformer
            = new QueryTransformer(varbinaryConstant)
            .prefix("E'\\\\\\\\x").suffix("'").groups("bytes");

    // Captures the use of VARCHAR(n BYTES) (in DDL)
    private static final Pattern varcharBytesDdl = Pattern.compile(
            "VARCHAR\\s*\\(\\s*(?<numBytes>\\w+)\\s+BYTES\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARCHAR(n BYTES), which PostgreSQL
    // does not support, and replaces it with VARCHAR(m), where m = n / 4
    // (but m is always at least 14, since many SQLCoverage tests use strings
    // of that length), which it does support
    private static final QueryTransformer varcharBytesDdlTransformer
            = new QueryTransformer(varcharBytesDdl)
            .prefix("VARCHAR(").suffix(")").multiplier(0.50).minimum(14)
            .groups("numBytes");

    // Captures the use of VARBINARY(n); however, this does not capture the use
    // of just VARBINARY, without a number of bytes in parentheses, although
    // VoltDB supports that syntax, because that would also capture some DDL
    // that should not be changed, such as table names R_VARBINARY_TABLE and
    // P_VARBINARY_TABLE
    private static final Pattern varbinaryDdl = Pattern.compile(
            "VARBINARY\\s*\\(\\s*\\d+\\s*\\)",
            Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing VARBINARY(n), which PostgreSQL does
    // not support, and replaces it with BYTEA, which it does support
    private static final QueryTransformer varbinaryDdlTransformer
            = new QueryTransformer(varbinaryDdl)
            .replacementText("BYTEA").useWholeMatch();

    // Captures the use of TINYINT (in DDL)
    private static final Pattern tinyintDdl = Pattern.compile(
            "TINYINT", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing TINYINT, which PostgreSQL does not
    // support, and replaces it with SMALLINT, which is an equivalent that
    // PostGIS does support
    private static final QueryTransformer tinyintDdlTransformer
            = new QueryTransformer(tinyintDdl)
            .replacementText("SMALLINT").useWholeMatch();

    // Captures the use of ASSUMEUNIQUE (in DDL)
    private static final Pattern assumeUniqueDdl = Pattern.compile(
            "ASSUMEUNIQUE", Pattern.CASE_INSENSITIVE);
    // Modifies a DDL statement containing ASSUMEUNIQUE, which PostgreSQL does
    // not support, and replaces it with UNIQUE, which is an equivalent that
    // PostGIS does support
    private static final QueryTransformer assumeUniqueDdlTransformer
            = new QueryTransformer(assumeUniqueDdl)
            .replacementText("UNIQUE").useWholeMatch();

    // Captures up to 6 table names, for each FROM clause used in the query
    // TODO: we may want to fix & finish this, in order to actually check the
    // column types, rather than just go by the column names (ENG-9945); this
    // would be used for for AVG, CEILING, FLOOR, and CAST queries
//    private static final Pattern tableNames = Pattern.compile(
//              "FROM\\s*\\(?<table1>\\w+)\\s*"
//            + "(\\s*,s*\\(?<table2>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table3>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table4>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table5>\\w+)\\s*)?"
//            + "(\\s*,s*\\(?<table6>\\w+)\\s*)?",
//            Pattern.CASE_INSENSITIVE);

    static public PostgreSQLBackend initializePostgreSQLBackend(CatalogContext context)
    {
        synchronized(backendLock) {
            if (m_backend == null) {
                try {
                    if (m_permanent_db_backend == null) {
                        m_permanent_db_backend = new PostgreSQLBackend();
                    }
                    Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                    stmt.execute("drop database if exists " + m_database_name + ";");
                    stmt.execute("create database " + m_database_name + ";");
                    m_backend = new PostgreSQLBackend(m_database_name);
                    final String binDDL = context.database.getSchema();
                    final String ddl = Encoder.decodeBase64AndDecompress(binDDL);
                    final String[] commands = ddl.split("\n");
                    for (String command : commands) {
                        String decoded_cmd = Encoder.hexDecodeToString(command);
                        decoded_cmd = decoded_cmd.trim();
                        if (decoded_cmd.length() == 0) {
                            continue;
                        }
                        m_backend.runDDL(decoded_cmd);
                    }
                }
                catch (final Exception e) {
                    hostLog.fatal("Unable to construct PostgreSQL backend");
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            return (PostgreSQLBackend) m_backend;
        }
    }

    /** Constructor specifying a (PostgreSQL) 'database', username and password. */
    public PostgreSQLBackend(String databaseName, String username, String password) {
        super("PostgreSQL", "org.postgresql.Driver",
                "jdbc:postgresql:" + databaseName,
                username, password);
    }

    /** Constructor specifying a username and password, with default (PostgreSQL) 'database'. */
    public PostgreSQLBackend(String username, String password) {
        this(m_permanent_database_name, username, password);
    }

    /** Constructor specifying a (PostgreSQL) 'database', with default username and password. */
    public PostgreSQLBackend(String databaseName) {
        this(databaseName, m_default_username, m_default_password);
    }

    /** Constructor using the default (PostgreSQL) 'database', username, and password. */
    public PostgreSQLBackend() {
        this(m_permanent_database_name);
    }

    /** Creates a new PostgreSQLBackend wrapping dbconn. This is (was?) used for testing only. */
    protected PostgreSQLBackend(Connection dbconn) {
        super(dbconn);
    }

    /** For a SQL DDL statement, replace (VoltDB) keywords not supported by
     *  PostgreSQL with other, similar terms. */
    public String transformDDL(String ddl) {
        return transformQuery(ddl, tinyintDdlTransformer,
                varcharBytesDdlTransformer, varbinaryDdlTransformer,
                assumeUniqueDdlTransformer);
    }

    /** For a SQL query, replace (VoltDB) keywords not supported by PostgreSQL,
     *  or which behave differently in PostgreSQL than in VoltDB, with other,
     *  similar terms, so that the results will match. */
    public String transformDML(String dml) {
        return transformQuery(dml, orderByQueryTransformer,
                avgQueryTransformer, ceilingOrFloorQueryTransformer,
                dayOfWeekQueryTransformer, dayOfYearQueryTransformer,
                stringConcatQueryTransformer, varbinaryConstantTransformer,
                upsertValuesQueryTransformer, upsertSelectQueryTransformer);
    }

    /** Optionally, modifies DDL statements in such a way that PostgreSQL
     *  results will match VoltDB results; and then passes the remaining
     *  work to the base class version. */
    protected void runDDL(String ddl, boolean transformDdl) {
        String modifiedDdl = (transformDdl ? transformDDL(ddl) : ddl);
        printTransformedSql(ddl, modifiedDdl);
        super.runDDL(modifiedDdl);
    }

    /** Modifies DDL statements in such a way that PostgreSQL results will
     *  match VoltDB results, and then passes the remaining work to the base
     *  class version. */
    @Override
    public void runDDL(String ddl) {
        runDDL(ddl, true);
    }

    /** Returns the column type name, in VoltDB, corresponding to the specified
     *  column type name in PostgreSQL. */
    @Override
    protected String getVoltColumnTypeName(String columnTypeName) {
        String equivalentTypeName = m_PostgreSQLTypeNames.get(columnTypeName);
        return (equivalentTypeName == null) ? columnTypeName : equivalentTypeName;
    }

    /**
     * Returns a VoltTable.ColumnInfo of appropriate type, based on a
     * <i>columnTypeName</i> and <i>colName</i> (both Strings).
     * This version checks for column types used only by PostgreSQL,
     * and then passes the remaining work to the base class version.
     */
    @Override
    protected VoltTable.ColumnInfo getColumnInfo(String columnTypeName, String colName) {
        return super.getColumnInfo(getVoltColumnTypeName(columnTypeName), colName);
    }

    /** Returns the specified String, after replacing certain "variables", such
     *  as {table} or {column:pk} (the ":pk" means primary keys only; ":npk"
     *  means non-primary-keys), in a QueryTransformer's prefix, suffix, or
     *  (group) replacement text, for which a corresponding group value will
     *  be substituted. In particular, this version makes substitutions to
     *  enable the transformation of VoltDB's UPSERT statements (both UPSERT
     *  INTO ... VALUES and UPSERT INTO ... SELECT) into PostgreSQL's equivalent,
     *  INSERT statements with an ON CONFLICT ... DO UPDATE clause, via the use
     *  of these variables, used in <i>upsertValuesQueryTransformer</i> and
     *  <i>upsertSelectQueryTransformer</i> (see above):
     *    {columns:pk}   : the primary key columns for the (main) table
     *    {columns:npk}  : the non-primary-key columns for the (main) table
     *    {values:npk}   : the corresponding values, or (for UPSERT INTO SELECT)
     *                     column names or expressions, to which those columns
     *                     in {columns:npk} should be set
     *    {table}        : the main table, into which data should be "upserted"
     *    {selecttables} : the table(s) in the SELECT clause
     *    {where:pk}     : the WHERE clause in the SELECT, involving primary
     *                     keys, which must replace any existing WHERE clause,
     *                     in order to transform the query for PostgreSQL
     *    {sort}         : the GROUP BY, HAVING, ORDER BY, LIMIT, and OFFSET
     *                     clauses (if any), at the end of the SELECT
     *  The first 2 select from among the columns in the UPSERT (after the main
     *  table name), if they were specified; otherwise, they select from all
     *  the columns of the (main) table. The last 3 only apply to an UPSERT
     *  INTO ... SELECT statement. */
    @Override
    protected String replaceGroupNameVariables(String str, List<String> groupNames, List<String> groupValues) {
        // If any of the inputs are null or empty, then never mind - just
        // return the original String (str)
        if (str == null || groupNames == null || groupValues == null ||
                str.isEmpty() || groupNames.isEmpty() || groupValues.isEmpty()) {
            return str;
        }

        // If a table was specified & found, use that; otherwise, never mind
        // (Note: this table represents the main table in an UPSERT statement,
        // e.g., "T1", in: UPSERT INTO T1 ...)
        String table = null;
        int index = groupNames.indexOf("table");
        if (index > -1 && index < groupValues.size()) {
            table = groupValues.get(index);
        } else {
            return str;
        }

        // If column values were specified & found, use those; otherwise, never
        // mind (Note: these column values may represent actual column values
        // specified in an UPSERT INTO VALUES statement; or column names or
        // expressions specified in an UPSERT INTO SELECT statement)
        List<String> columnValues = null;
        index = groupNames.indexOf("values");
        if (index > -1 && index < groupValues.size()) {
            columnValues = new ArrayList<String>(Arrays.asList(groupValues.get(index).split(",")));
        } else {
            return str;
        }

        // Get the primary key column names, and the non-primary-key column
        // names, and all column names, for the specified table
        List<String> primaryKeyColumns = getPrimaryKeys(table);
        List<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(table);
        List<String> allColumns = getAllColumns(table);

        // If one or more "select tables" was specified & found (that is, tables
        // used in the SELECT part of an UPSERT INTO T1 SELECT... statement),
        // use those; otherwise, this probably won't be needed, but use the main
        // (UPSERT INTO) table we already found, just in case
        List<String> selectTables = null;
        index = groupNames.indexOf("selecttables");
        if (index > -1 && index < groupValues.size()) {
            String selectTablesStr = groupValues.get(index).toUpperCase();
            if (selectTablesStr.contains(" JOIN ")) {
                selectTables = new ArrayList<String>(Arrays.asList(selectTablesStr.split(" JOIN ")));
            } else {
                selectTables = new ArrayList<String>(Arrays.asList(groupValues.get(index).split(",")));
            }
        } else {
            selectTables = Arrays.asList(table);
        }

        // If particular columns were specified, use only those; otherwise use
        // all columns (primary keys and non-primary-keys), as found above
        List<String> columns = null;
        index = groupNames.indexOf("columns");
        if (index > -1 && index < groupValues.size() && groupValues.get(index) != null) {
            columns = Arrays.asList(groupValues.get(index).split(","));
            // Lower-case table and column names are required for PostgreSQL;
            // we might need to alter this if we use another comparison
            // database (besides HSQL) someday
            for (int i=0; i < columns.size(); i++) {
                columns.set(i, columns.get(i).trim().toLowerCase());
            }
            // Retain only those primary key columns that are in the specified
            // list of columns - in the specified column list order
            List<String> temp = new ArrayList<String>(columns);
            temp.retainAll(primaryKeyColumns);
            primaryKeyColumns = temp;
            // Retain only those non-primary-key columns that are in the specified
            // list of columns - in the specified column list order
            temp = new ArrayList<String>(columns);
            temp.retainAll(nonPrimaryKeyColumns);
            nonPrimaryKeyColumns = temp;
        } else {
            columns = getAllColumns(table);
        }

        // Handle the special case of UPSERT INTO ... SELECT *, i.e., when one
        // (or more??) of the "columnValues" is equal to "*"; go in reverse
        // order, to avoid messing up the list
        for (int i = columnValues.size() - 1; i >= 0; i--) {
            if (columnValues.get(i).trim().equals("*")) {
                columnValues.remove(i);
                for (int t = selectTables.size() - 1; t >= 0; t--) {
                    columnValues.addAll(i, getAllColumns(selectTables.get(t).trim()));
                }
            }
        }

        // Remove elements from the "columnValues" list that correspond to
        // primary key columns, since those values do not need to be set in an
        // INSERT statement's ON CONFLICT ... DO UPDATE clause; but add them to
        // a separate list (which is needed only for an UPSERT INTO SELECT
        // statement); go in reverse order, to avoid messing up the list; the
        // "columnValues" and "columns" lists should have the same size here,
        // but just in case use the minimum
        List<String> pkColumnValues = new ArrayList<String>();
        for (int i = Math.min(columnValues.size(), columns.size()) - 1; i >= 0; i--) {
            if (primaryKeyColumns.contains(columns.get(i))) {
                pkColumnValues.add(0, columnValues.get(i));
                columnValues.remove(i);
            }
        }

        // If "where" was included in the "groupNames" (which it would be for an
        // UPSERT INTO SELECT statement), prepare a WHERE clause, setting primary
        // keys equal to their equivalent values in the main (_TMP) table, e.g.:
        //     "WHERE id=_TMP.id AND foo=_TMP.blah AND bar=_TMP.yada "
        String pkWhereClause = "EMPTY";
        if (groupNames.indexOf("where") > -1 && pkColumnValues.size() > 0
                && primaryKeyColumns.size() > 0) {
            pkWhereClause = "WHERE " + pkColumnValues.get(0)+"=_TMP."+primaryKeyColumns.get(0)+" ";
            for (int i=1; i < pkColumnValues.size(); i++) {
                pkWhereClause += "AND " + pkColumnValues.get(i)+"=_TMP."+primaryKeyColumns.get(i)+" ";
            }
        }

        // Replace the groupName "variables" with their corresponding
        // groupValues, processing special cases involving columnType
        // "pk" or "npk", as needed
        StringBuffer modified_str = new StringBuffer();
        Matcher matcher = groupNameVariables.matcher(str);
        while (matcher.find()) {
            String groupName = matcher.group("groupName");
            String columnType = matcher.group("columnType");
            // Filter this "variable" to only include primary key columns
            if ("pk".equalsIgnoreCase(columnType)) {
                if ("columns".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, String.join(", ", primaryKeyColumns));
                } else if ("where".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, pkWhereClause);
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+":pk}");
                }
            // Filter this "variable" to only include non-primary-key columns
            } else if ("npk".equalsIgnoreCase(columnType)) {
                if ("columns".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, String.join(", ", nonPrimaryKeyColumns));
                } else if ("values".equalsIgnoreCase(groupName)) {
                    matcher.appendReplacement(modified_str, String.join(", ", columnValues));
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+":npk}");
                }
            // Simply return the value of this "variable"
            } else {
                index = groupNames.indexOf(groupName);
                if (index > -1 && index < groupValues.size()) {
                    matcher.appendReplacement(modified_str, groupValues.get(index));
                } else {
                    // No match: give up on this "variable"
                    matcher.appendReplacement(modified_str, "{"+groupName+"}");
                }
            }
        }
        matcher.appendTail(modified_str);
        return modified_str.toString();
    }

    /** Optionally, modifies queries in such a way that PostgreSQL results will
     *  match VoltDB results; and then passes the remaining work to the base
     *  class version. */
    protected VoltTable runDML(String dml, boolean transformDml) {
        String modifiedDml = (transformDml ? transformDML(dml) : dml);
        printTransformedSql(dml, modifiedDml);
        return super.runDML(modifiedDml);
    }

    /** Modifies queries in such a way that PostgreSQL results will match VoltDB
     *  results, and then passes the remaining work to the base class version. */
    @Override
    public VoltTable runDML(String dml) {
        return runDML(dml, true);
    }

    protected Connection getConnection() {
        return dbconn;
    }

    @Override
    protected void shutdown() {
        try {
            dbconn.close();
            dbconn = null;
            System.gc();
            try {
                Statement stmt = m_permanent_db_backend.getConnection().createStatement();
                stmt.execute("drop database if exists " + m_database_name + ";");
            } catch (SQLException ex) {
                System.err.println("In PostgreSQLBackend.shutdown(), caught exception: " + ex);
                ex.printStackTrace();
            }
            if (transformedSqlFileWriter != null) {
                transformedSqlFileWriter.close();
                transformedSqlFileWriter = null;
            }
        } catch (Exception e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_Backend_ErrorOnShutdown.name(), e);
        }
    }

}
