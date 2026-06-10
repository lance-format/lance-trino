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

import java.util.List;

import static java.util.Objects.requireNonNull;

final class LanceFieldPath
{
    private LanceFieldPath() {}

    static String canonicalPath(List<String> path)
    {
        requireNonNull(path, "path is null");
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path is empty");
        }
        return path.stream()
                .map(LanceFieldPath::escapePathPart)
                .collect(java.util.stream.Collectors.joining("."));
    }

    static String rootName(String canonicalPath)
    {
        requireNonNull(canonicalPath, "canonicalPath is null");
        StringBuilder root = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < canonicalPath.length(); i++) {
            char c = canonicalPath.charAt(i);
            if (escaped) {
                root.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '.') {
                return root.toString();
            }
            root.append(c);
        }
        if (escaped) {
            root.append('\\');
        }
        return root.toString();
    }

    private static String escapePathPart(String part)
    {
        requireNonNull(part, "part is null");
        return part.replace("\\", "\\\\")
                .replace(".", "\\.");
    }
}
