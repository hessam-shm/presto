package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.type.Type;

import static com.facebook.presto.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;

public class ColumnMapping {
    public static ColumnMapping booleanMapping(Type prestoType, BooleanReadFunction readFunction, BooleanWriteFunction writeFunction)
    {
        return booleanMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping booleanMapping(
            Type prestoType,
            BooleanReadFunction readFunction,
            BooleanWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping longMapping(Type prestoType, LongReadFunction readFunction, LongWriteFunction writeFunction)
    {
        return longMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping longMapping(
            Type prestoType,
            LongReadFunction readFunction,
            LongWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping doubleMapping(Type prestoType, DoubleReadFunction readFunction, DoubleWriteFunction writeFunction)
    {
        return doubleMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping doubleMapping(
            Type prestoType,
            DoubleReadFunction readFunction,
            DoubleWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping sliceMapping(Type prestoType, SliceReadFunction readFunction, SliceWriteFunction writeFunction)
    {
        return sliceMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping sliceMapping(
            Type prestoType,
            SliceReadFunction readFunction,
            SliceWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static <T> ColumnMapping objectMapping(Type prestoType, ObjectReadFunction readFunction, ObjectWriteFunction writeFunction)
    {
        return objectMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static <T> ColumnMapping objectMapping(
            Type prestoType,
            ObjectReadFunction readFunction,
            ObjectWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    private final Type type;
    private final ReadFunction readFunction;
    private final WriteFunction writeFunction;
    private final PredicatePushdownController predicatePushdownController;

    /**
     * @deprecated Prefer factory methods instead over calling constructor directly.
     */
    @Deprecated
    public ColumnMapping(Type type, ReadFunction readFunction, WriteFunction writeFunction, PredicatePushdownController predicatePushdownController)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFunction = requireNonNull(readFunction, "readFunction is null");
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
        checkArgument(
                type.getJavaType() == readFunction.getJavaType(),
                "Trino type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
        checkArgument(
                type.getJavaType() == writeFunction.getJavaType(),
                "Trino type %s is not compatible with write function %s accepting %s",
                type,
                writeFunction,
                writeFunction.getJavaType());
        this.predicatePushdownController = requireNonNull(predicatePushdownController, "pushdownController is null");
    }

    public Type getType()
    {
        return type;
    }

    public ReadFunction getReadFunction()
    {
        return readFunction;
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }

    public PredicatePushdownController getPredicatePushdownController()
    {
        return predicatePushdownController;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
