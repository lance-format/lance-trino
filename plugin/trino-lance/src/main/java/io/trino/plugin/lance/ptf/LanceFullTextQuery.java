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
package io.trino.plugin.lance.ptf;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import org.lance.ipc.FullTextQuery;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record LanceFullTextQuery(
        List<String> columns,
        String query,
        boolean operatorAnd,
        Optional<Integer> fuzziness,
        float boost,
        int prefixLength,
        int maxExpansions)
{
    public LanceFullTextQuery
    {
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(query, "query is null");
        requireNonNull(fuzziness, "fuzziness is null");
    }

    public static LanceFullTextQuery parse(List<String> columns, String query, String options)
    {
        boolean operatorAnd = false;
        Optional<Integer> fuzziness = Optional.empty();
        float boost = 1.0f;
        int prefixLength = 0;
        int maxExpansions = 50;
        if (options != null && !options.isBlank()) {
            for (String option : options.split(",")) {
                String[] parts = option.split("=", 2);
                if (parts.length != 2) {
                    throw invalid("Invalid option: " + option.trim());
                }
                String key = parts[0].trim().toLowerCase(ENGLISH);
                String value = parts[1].trim();
                try {
                    switch (key) {
                        case "operator" -> operatorAnd = parseOperatorAnd(value);
                        case "fuzziness" -> fuzziness = Optional.of(nonNegative(key, parseInt(value)));
                        case "boost" -> boost = parseFloat(value);
                        case "prefix_length" -> prefixLength = nonNegative(key, parseInt(value));
                        case "max_expansions" -> maxExpansions = parseInt(value);
                        default -> throw invalid("Unknown option: " + key);
                    }
                }
                catch (NumberFormatException e) {
                    throw invalid("Invalid value for " + key + ": " + value);
                }
            }
        }
        if (maxExpansions < 1) {
            throw invalid("max_expansions must be at least 1");
        }
        if (columns.size() > 1 && (fuzziness.isPresent() || prefixLength != 0 || boost != 1.0f || maxExpansions != 50)) {
            throw invalid("Only the operator option is supported with multiple columns");
        }
        return new LanceFullTextQuery(columns, query, operatorAnd, fuzziness, boost, prefixLength, maxExpansions);
    }

    private static boolean parseOperatorAnd(String value)
    {
        return switch (value.toUpperCase(ENGLISH)) {
            case "AND" -> true;
            case "OR" -> false;
            default -> throw invalid("operator must be AND or OR: " + value);
        };
    }

    private static int nonNegative(String key, int value)
    {
        if (value < 0) {
            throw invalid(key + " must not be negative");
        }
        return value;
    }

    private static TrinoException invalid(String message)
    {
        return new TrinoException(INVALID_FUNCTION_ARGUMENT, message);
    }

    public FullTextQuery toLanceQuery()
    {
        FullTextQuery.Operator operator = operatorAnd ? FullTextQuery.Operator.AND : FullTextQuery.Operator.OR;
        if (columns.size() == 1) {
            return FullTextQuery.match(query, columns.getFirst(), boost, fuzziness, maxExpansions, operator, prefixLength);
        }
        return FullTextQuery.multiMatch(query, columns, null, operator);
    }
}
