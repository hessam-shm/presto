package com.facebook.presto.plugin.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface BooleanWriteFunction extends WriteFunction{

    @Override
    default Class<?> getJavaType()
    {
        return boolean.class;
    }

    void set(PreparedStatement statement, int index, boolean value)
            throws SQLException;
}
