/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.memsql;

import com.facebook.presto.common.type.*;
import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MILLIS;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.mysql.jdbc.SQLError.SQL_STATE_ER_TABLE_EXISTS_ERROR;
import static com.mysql.jdbc.SQLError.SQL_STATE_SYNTAX_ERROR;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MemSqlClient
        extends BaseJdbcClient
{

    private final Type jsonType;

    @Inject
    public MemSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ConnectionFactory connectionFactory, TypeManager typeManager)
            throws SQLException
    {
        super(connectorId, config, "`", connectionFactory);
        requireNonNull(typeManager, "typeManager is null");
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        // for MemSQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean filterSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("memsql")) {
            return false;
        }
        return true;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MemSQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, Optional.of(metadata.getSearchStringEscape())).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MemSQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    //TODO: use default type handler just for test
    /*@Override
    protected String toSqlType(Type typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> unsignedMapping = getUnsignedMapping(typeHandle);
        if (unsignedMapping.isPresent()) {
            return unsignedMapping;
        }

        if (jdbcTypeName.equalsIgnoreCase("json")) {
            return Optional.of(jsonColumnMapping());
        }

        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                int varcharLength = typeHandle.getRequiredColumnSize();
                VarcharType varcharType = (varcharLength <= VarcharType.MAX_LENGTH) ? createVarcharType(varcharLength) : createUnboundedVarcharType();
                // Remote database can be case insensitive.
                PredicatePushdownController predicatePushdownController = PUSHDOWN_AND_KEEP;
                return Optional.of(ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), predicatePushdownController));
            case Types.DECIMAL:
                int precision = typeHandle.getRequiredColumnSize();
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
        }

        // TODO add explicit mappings
        return legacyToPrestoType(session, connection, typeHandle);
    }*/

    //TODO: referred in toSqlType method
    /*protected Optional<ColumnMapping> getForcedMappingToVarchar(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isPresent() && jdbcTypesMappedToVarchar.contains(typeHandle.getJdbcTypeName().get())) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }*/

    protected static Optional<ColumnMapping> mapToUnboundedVarchar(Type typeHandle)
    {
        VarcharType unboundedVarcharType = VarcharType.createVarcharType(21844);
        return Optional.of(ColumnMapping.sliceMapping(
                unboundedVarcharType,
                getSlice(unboundedVarcharType),
                (statement, index, value) -> {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            "Underlying type that is mapped to VARCHAR is not supported for INSERT: " + typeHandle.getDisplayName());
                },
                DISABLE_PUSHDOWN));
    }

    private static SliceReadFunction getSlice(VarcharType varcharType){
        return (ResultSet resultSet, int columnIndex) -> {
            Slice slice = null;
            try {
                slice = utf8Slice(resultSet.getString(columnIndex));
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            checkLengthInCodePoints(slice, varcharType, 21844);
            return slice;
        };
    }

    private static void checkLengthInCodePoints(Slice value, Type characterDataType, int lengthLimit)
    {
        // Quick check in bytes
        if (value.length() <= lengthLimit) {
            return;
        }
        // Actual check
        if (countCodePoints(value) <= lengthLimit) {
            return;
        }
        throw new IllegalStateException(format(
                "Illegal value for type %s: '%s' [%s]",
                characterDataType,
                value.toStringUtf8(),
                base16().encode(value.getBytes())));
    }

    //TODO: add write capabality
    /*@Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // MemSQL doesn't accept `some;column` in CTAS statements - so we explicitly block it and throw a proper error message
        tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .filter(s -> s.contains(";"))
                .findAny()
                .ifPresent(illegalColumnName -> {
                    throw new PrestoException(JDBC_ERROR, illegalColumnName));
                });

        super.createTable(session, tableMetadata);
    }*/

    //TODO: add write capabilities
    /*@Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            // MemSQL versions earlier than 5.7 do not support the CHANGE syntax
            String sql = format(
                    "ALTER TABLE %s CHANGE %s %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }*/

    //TODO: add write capabilities
    /*@Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // MemSQL doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        verify(handle.getSchemaName() == null);
        renameTable(session, null, handle.getCatalogName(), handle.getTableName(), newTableName);
    }*/

    //TODO: referred in toSqlType method
    /*private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isEmpty()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(smallintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20)));
        }

        return Optional.empty();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }*/
}
