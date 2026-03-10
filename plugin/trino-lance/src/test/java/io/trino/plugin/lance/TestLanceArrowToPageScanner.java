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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

/**
 * Exercises every type path in LanceArrowToPageScanner.convertType using a wide dataset
 * (wide_types_table.lance). Each assertion targets a single (column, row) pair.
 *
 * Dataset schema and Arrow→Trino mappings:
 *   id           int64               → BIGINT             (BigIntVector)
 *   col_bool     bool                → BOOLEAN            (BitVector)
 *   col_int32    int32               → INTEGER            (IntVector)
 *   col_int64    int64               → BIGINT             (BigIntVector, signed)
 *   col_uint64   uint64              → BIGINT             (UInt8Vector, unsigned)
 *   col_float16  float16             → REAL               (Float2Vector widened to float32)
 *   col_float32  float32             → REAL               (Float4Vector)
 *   col_float64  float64             → DOUBLE             (Float8Vector)
 *   col_string   utf8                → VARCHAR            (VarCharVector)
 *   col_binary   binary              → VARBINARY          (VarBinaryVector)
 *   col_date     date32              → DATE               (DateDayVector, days since epoch)
 *   col_ts       timestamp[us]       → TIMESTAMP_TZ_MILLIS (Lance JNI promotes to UTC)
 *   col_ts_tz    timestamp[us, UTC]  → TIMESTAMP_TZ_MILLIS (TimeStampMicroTZVector)
 *   col_list_f32 list(float32)        → ARRAY(REAL)        (ListVector + Float4Vector)
 *   col_fsl_f32  fsl(float32)[3]     → ARRAY(REAL)        (FixedSizeListVector + Float4Vector)
 *   col_fsl_f16  fsl(float16)[3]     → ARRAY(REAL)        (FixedSizeListVector + Float2Vector widened)
 */
@TestInstance(PER_METHOD)
public class TestLanceArrowToPageScanner
{
    private static final SchemaTableName WIDE_TABLE = new SchemaTableName("default", "wide_types_table");

    // 2024-01-15 10:30:00 UTC
    private static final long ROW0_DATE_DAYS = 19737L;
    private static final long ROW0_TS_MILLIS = 1705314600000L;

    // 2024-06-30 20:00:00 UTC
    private static final long ROW1_DATE_DAYS = 19904L;
    private static final long ROW1_TS_MILLIS = 1719777600000L;

    private LanceMetadata metadata;
    private LanceSplitManager splitManager;
    private LanceRuntime runtime;
    private Page page;
    private List<LanceColumnHandle> columns;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceURL = Resources.getResource(TestLanceArrowToPageScanner.class, "/example_db");
        assertThat(lanceURL).describedAs("example db is null").isNotNull();
        LanceConfig lanceConfig = new LanceConfig().setSingleLevelNs(true);
        Map<String, String> catalogProperties = ImmutableMap.of("lance.root", lanceURL.toString());
        runtime = new LanceRuntime(lanceConfig, catalogProperties);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
        JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
        metadata = new LanceMetadata(runtime, lanceConfig, commitTaskDataCodec, mergeCommitDataCodec);
        splitManager = new LanceSplitManager(runtime, lanceConfig);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                TestingConnectorSession.SESSION, WIDE_TABLE, Optional.empty(), Optional.empty());
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        ConnectorSplitSource splits = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, tableHandle, null, null);
        LanceSplit lanceSplit = (LanceSplit) splits.getNextBatch(10).get().getSplits().get(0);

        columns = runtime.getColumnHandleList(null, lanceTableHandle.getTablePath(), null, Collections.emptyMap());

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                lanceTableHandle, columns, lanceSplit.getFragments(), Collections.emptyMap(), 8192, null, runtime)) {
            page = pageSource.getNextPage();
        }

        assertThat(page).isNotNull();
        assertThat(page.getPositionCount()).isEqualTo(2);
    }

    @Test
    public void testBigint()
    {
        assertBigint("id", 0, 1L);
        assertBigint("id", 1, 2L);
    }

    @Test
    public void testBoolean()
    {
        assertBoolean("col_bool", 0, true);
        assertBoolean("col_bool", 1, false);
    }

    @Test
    public void testInteger()
    {
        assertInteger("col_int32", 0, 10);
        assertInteger("col_int32", 1, -10);
    }

    @Test
    public void testBigintSigned()
    {
        assertBigint("col_int64", 0, 100L);
        assertBigint("col_int64", 1, -100L);
    }

    @Test
    public void testBigintUnsigned()
    {
        // uint64 stored in UInt8Vector, read as signed long
        assertBigint("col_uint64", 0, 42L);
        assertBigint("col_uint64", 1, 99L);
    }

    @Test
    public void testFloat16WideningToReal()
    {
        // Float2Vector widened to REAL (float32)
        assertReal("col_float16", 0, 3.5f);
        assertReal("col_float16", 1, -3.5f);
    }

    @Test
    public void testFloat32()
    {
        assertReal("col_float32", 0, 1.5f);
        assertReal("col_float32", 1, -1.5f);
    }

    @Test
    public void testFloat64()
    {
        assertDouble("col_float64", 0, 2.5);
        assertDouble("col_float64", 1, -2.5);
    }

    @Test
    public void testVarchar()
    {
        assertVarchar("col_string", 0, "hello");
        assertVarchar("col_string", 1, "world");
    }

    @Test
    public void testVarbinary()
    {
        assertVarbinary("col_binary", 0, new byte[] {0x01, 0x02});
        assertVarbinary("col_binary", 1, new byte[] {0x03, 0x04});
    }

    @Test
    public void testDate()
    {
        assertDate("col_date", 0, ROW0_DATE_DAYS);
        assertDate("col_date", 1, ROW1_DATE_DAYS);
    }

    @Test
    public void testTimestampNoTz()
    {
        // Lance JNI promotes timestamp[us] (no TZ) to UTC internally → TIMESTAMP_TZ_MILLIS
        assertTimestampTzMillis("col_ts", 0, ROW0_TS_MILLIS);
        assertTimestampTzMillis("col_ts", 1, ROW1_TS_MILLIS);
    }

    @Test
    public void testTimestampWithTz()
    {
        assertTimestampTzMillis("col_ts_tz", 0, ROW0_TS_MILLIS);
        assertTimestampTzMillis("col_ts_tz", 1, ROW1_TS_MILLIS);
    }

    @Test
    public void testListFloat32()
    {
        assertArrayReal("col_list_f32", 0, new float[] {1.0f, 2.0f});
        assertArrayReal("col_list_f32", 1, new float[] {3.0f, 4.0f, 5.0f});
    }

    @Test
    public void testFixedSizeListFloat32()
    {
        assertArrayReal("col_fsl_f32", 0, new float[] {1.0f, 2.0f, 3.0f});
        assertArrayReal("col_fsl_f32", 1, new float[] {4.0f, 5.0f, 6.0f});
    }

    @Test
    public void testFixedSizeListFloat16WideningToReal()
    {
        // Float2Vector elements widened to REAL inside a FixedSizeListVector
        assertArrayReal("col_fsl_f16", 0, new float[] {7.0f, 8.0f, 9.0f});
        assertArrayReal("col_fsl_f16", 1, new float[] {10.0f, 11.0f, 12.0f});
    }

    // --- per-value assertion helpers ---

    private Block blockFor(String name)
    {
        int idx = columns.stream().map(LanceColumnHandle::name).toList().indexOf(name);
        assertThat(idx).describedAs("column not found: " + name).isGreaterThanOrEqualTo(0);
        return page.getBlock(idx);
    }

    private void assertBigint(String col, int row, long expected)
    {
        assertThat(BIGINT.getLong(blockFor(col), row)).isEqualTo(expected);
    }

    private void assertBoolean(String col, int row, boolean expected)
    {
        assertThat(BOOLEAN.getBoolean(blockFor(col), row)).isEqualTo(expected);
    }

    private void assertInteger(String col, int row, int expected)
    {
        assertThat((int) INTEGER.getLong(blockFor(col), row)).isEqualTo(expected);
    }

    private void assertReal(String col, int row, float expected)
    {
        float actual = Float.intBitsToFloat((int) REAL.getLong(blockFor(col), row));
        assertThat(actual).isCloseTo(expected, within(0.001f));
    }

    private void assertDouble(String col, int row, double expected)
    {
        assertThat(DOUBLE.getDouble(blockFor(col), row)).isCloseTo(expected, within(0.001));
    }

    private void assertVarchar(String col, int row, String expected)
    {
        assertThat(VARCHAR.getSlice(blockFor(col), row).toStringUtf8()).isEqualTo(expected);
    }

    private void assertVarbinary(String col, int row, byte[] expected)
    {
        Slice slice = VARBINARY.getSlice(blockFor(col), row);
        assertThat(slice.getBytes()).isEqualTo(expected);
    }

    private void assertDate(String col, int row, long expectedDays)
    {
        assertThat(DATE.getLong(blockFor(col), row)).isEqualTo(expectedDays);
    }

    private void assertTimestampTzMillis(String col, int row, long expectedMillis)
    {
        long packed = TIMESTAMP_TZ_MILLIS.getLong(blockFor(col), row);
        assertThat(unpackMillisUtc(packed)).isEqualTo(expectedMillis);
    }

    private void assertArrayReal(String col, int row, float[] expectedElements)
    {
        LanceColumnHandle handle = columns.stream()
                .filter(c -> c.name().equals(col))
                .findFirst()
                .orElseThrow(() -> new AssertionError("column not found: " + col));
        ArrayType arrayType = (ArrayType) handle.trinoType();
        Block inner = (Block) arrayType.getObject(blockFor(col), row);
        assertThat(inner.getPositionCount()).isEqualTo(expectedElements.length);
        for (int i = 0; i < expectedElements.length; i++) {
            float actual = Float.intBitsToFloat((int) REAL.getLong(inner, i));
            assertThat(actual).isCloseTo(expectedElements[i], within(0.001f));
        }
    }
}
