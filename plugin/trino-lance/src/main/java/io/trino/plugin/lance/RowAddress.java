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

/**
 * Utility class for working with Lance row addresses.
 * A row address is a 64-bit value that uniquely identifies a row in a dataset:
 * - High 32 bits: Fragment ID
 * - Low 32 bits: Row index within the fragment
 */
public final class RowAddress
{
    public static final String LANCE_ROW_ADDRESS = "$row_address";
    public static final String LANCE_FRAGMENT_ID = "$fragment_id";

    private RowAddress() {}

    public static long encode(int fragmentId, int rowIndex)
    {
        return ((long) fragmentId << 32) | (rowIndex & 0xFFFFFFFFL);
    }

    public static int fragmentId(long rowAddress)
    {
        return (int) (rowAddress >> 32);
    }

    public static int rowIndex(long rowAddress)
    {
        return (int) rowAddress;
    }
}
