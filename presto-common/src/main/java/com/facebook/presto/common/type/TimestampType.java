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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

//
// A timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC.  When performing calculations
// on a timestamp the client's time zone must be taken into account.
//
public final class TimestampType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;

    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    private static final TimestampType[] TYPES = new TimestampType[MAX_PRECISION + 1];

    public static final TimestampType TIMESTAMP = new TimestampType();
    public static final TimestampType TIMESTAMP_SECONDS = createTimestampType(0);
    public static final TimestampType TIMESTAMP_MILLIS = createTimestampType(3);
    public static final TimestampType TIMESTAMP_MICROS = createTimestampType(6);
    public static final TimestampType TIMESTAMP_NANOS = createTimestampType(9);
    public static final TimestampType TIMESTAMP_PICOS = createTimestampType(12);

    private TimestampType()
    {
        super(parseTypeSignature(StandardTypes.TIMESTAMP));
    }

    public static TimestampType createTimestampType(int precision) {
        return TYPES[precision];
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (properties.isLegacyTimestamp()) {
            return new SqlTimestamp(block.getLong(position), properties.getTimeZoneKey());
        }
        else {
            return new SqlTimestamp(block.getLong(position));
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == TIMESTAMP;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
