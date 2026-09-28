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
package io.trino.plugin.lance;

import io.trino.spi.type.RowType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class BlobUtils
{
    public static final String LANCE_ENCODING_BLOB_KEY = "lance-encoding:blob";
    public static final String LANCE_ENCODING_BLOB_VALUE = "true";

    public static final String ARROW_EXTENSION_NAME_KEY = "ARROW:extension:name";
    public static final String LANCE_BLOB_V2_EXTENSION_NAME = "lance.blob.v2";

    public static final String BLOB_POSITION_SUFFIX = "__blob_pos";
    public static final String BLOB_SIZE_SUFFIX = "__blob_size";

    public static final String LANCE_ENCODING_PROPERTY_SUFFIX = ".lance.encoding";

    public enum BlobVirtualColumnType
    {
        NONE,
        POSITION,
        SIZE
    }

    // Descriptor struct Lance returns when scanning a blob v2 column: kind, position, size, blob_id, blob_uri.
    // Unsigned Arrow types are widened the same way as regular columns (uint8 -> SMALLINT, uint32/uint64 -> BIGINT).
    public static final RowType BLOB_V2_DESCRIPTOR_TYPE = RowType.from(List.of(
            new RowType.Field(Optional.of("kind"), SMALLINT),
            new RowType.Field(Optional.of("position"), BIGINT),
            new RowType.Field(Optional.of("size"), BIGINT),
            new RowType.Field(Optional.of("blob_id"), BIGINT),
            new RowType.Field(Optional.of("blob_uri"), VARCHAR)));

    private BlobUtils() {}

    public static boolean isBlobArrowField(Field field)
    {
        if (field == null) {
            return false;
        }

        Map<String, String> metadata = field.getMetadata();
        if (metadata == null || metadata.isEmpty()) {
            return false;
        }

        if (!metadata.containsKey(LANCE_ENCODING_BLOB_KEY)) {
            return false;
        }

        String value = metadata.get(LANCE_ENCODING_BLOB_KEY);
        return LANCE_ENCODING_BLOB_VALUE.equalsIgnoreCase(value);
    }

    public static boolean isBlobV2Field(Field field)
    {
        return field != null && isBlobV2Metadata(field.getMetadata());
    }

    public static boolean isBlobV2Metadata(Map<String, String> metadata)
    {
        return metadata != null && LANCE_BLOB_V2_EXTENSION_NAME.equals(metadata.get(ARROW_EXTENSION_NAME_KEY));
    }

    public static boolean isBlobStructField(Field field)
    {
        if (field == null) {
            return false;
        }
        if (!(field.getType() instanceof ArrowType.Struct)) {
            return false;
        }
        return isBlobArrowField(field);
    }

    public static String getBlobPositionColumnName(String columnName)
    {
        return columnName + BLOB_POSITION_SUFFIX;
    }

    public static String getBlobSizeColumnName(String columnName)
    {
        return columnName + BLOB_SIZE_SUFFIX;
    }

    public static boolean isBlobVirtualColumn(String columnName)
    {
        return columnName.endsWith(BLOB_POSITION_SUFFIX) || columnName.endsWith(BLOB_SIZE_SUFFIX);
    }

    public static BlobVirtualColumnType getBlobVirtualColumnType(String columnName)
    {
        if (columnName.endsWith(BLOB_POSITION_SUFFIX)) {
            return BlobVirtualColumnType.POSITION;
        }
        else if (columnName.endsWith(BLOB_SIZE_SUFFIX)) {
            return BlobVirtualColumnType.SIZE;
        }
        return BlobVirtualColumnType.NONE;
    }

    public static String getBaseBlobColumnName(String virtualColumnName)
    {
        if (virtualColumnName.endsWith(BLOB_POSITION_SUFFIX)) {
            return virtualColumnName.substring(0, virtualColumnName.length() - BLOB_POSITION_SUFFIX.length());
        }
        else if (virtualColumnName.endsWith(BLOB_SIZE_SUFFIX)) {
            return virtualColumnName.substring(0, virtualColumnName.length() - BLOB_SIZE_SUFFIX.length());
        }
        return virtualColumnName;
    }

    public static String getBlobEncodingPropertyKey(String columnName)
    {
        return columnName + LANCE_ENCODING_PROPERTY_SUFFIX;
    }
}
