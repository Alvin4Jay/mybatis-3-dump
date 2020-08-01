/**
 * Copyright 2009-2018 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ibatis.scripting.xmltags;

import org.apache.ibatis.session.Configuration;

import java.util.*;

/**
 * 存储SQL中<trim>节点的内容
 *
 * @author Clinton Begin
 */
public class TrimSqlNode implements SqlNode {

    private final SqlNode contents;
    private final String prefix;
    private final String suffix;
    private final List<String> prefixesToOverride;
    private final List<String> suffixesToOverride;
    private final Configuration configuration;

    /**
     * @param configuration      全局配置
     * @param contents           <trim>节点的内部节点内容，MixedSqlNode
     * @param prefix             前缀
     * @param prefixesToOverride 去除的多余前缀
     * @param suffix             后缀
     * @param suffixesToOverride 去除的多余后缀
     */
    public TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, String prefixesToOverride,
                       String suffix, String suffixesToOverride) {
        this(configuration, contents, prefix, parseOverrides(prefixesToOverride), suffix,
            parseOverrides(suffixesToOverride));
    }

    protected TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, List<String> prefixesToOverride,
                          String suffix, List<String> suffixesToOverride) {
        this.contents = contents;
        this.prefix = prefix;
        this.prefixesToOverride = prefixesToOverride;
        this.suffix = suffix;
        this.suffixesToOverride = suffixesToOverride;
        this.configuration = configuration;
    }

    /** 解析需要覆盖的内容， | 分隔 */
    private static List<String> parseOverrides(String overrides) {
        if (overrides != null) {
            final StringTokenizer parser = new StringTokenizer(overrides, "|", false);
            final List<String> list = new ArrayList<>(parser.countTokens());
            while (parser.hasMoreTokens()) {
                // 转为大写
                list.add(parser.nextToken().toUpperCase(Locale.ENGLISH));
            }
            return list;
        }
        return Collections.emptyList();
    }

    @Override
    public boolean apply(DynamicContext context) {
        // 创建具有过滤功能的 DynamicContext
        FilteredDynamicContext filteredDynamicContext = new FilteredDynamicContext(context);
        // 解析<trim>内部节点的SQL片段
        boolean result = contents.apply(filteredDynamicContext);
        // 过滤掉前缀和后缀
        filteredDynamicContext.applyAll();
        return result;
    }

    private class FilteredDynamicContext extends DynamicContext {
        private DynamicContext delegate;
        private boolean prefixApplied;
        private boolean suffixApplied;
        private StringBuilder sqlBuffer;

        public FilteredDynamicContext(DynamicContext delegate) {
            super(configuration, null);
            this.delegate = delegate;
            this.prefixApplied = false;
            this.suffixApplied = false;
            this.sqlBuffer = new StringBuilder();
        }

        public void applyAll() {
            sqlBuffer = new StringBuilder(sqlBuffer.toString().trim());
            // 大写SQL
            String trimmedUppercaseSql = sqlBuffer.toString().toUpperCase(Locale.ENGLISH);
            if (trimmedUppercaseSql.length() > 0) {
                applyPrefix(sqlBuffer, trimmedUppercaseSql);
                applySuffix(sqlBuffer, trimmedUppercaseSql);
            }
            // 将处理后的sql片段追加到原context
            delegate.appendSql(sqlBuffer.toString());
        }

        @Override
        public Map<String, Object> getBindings() {
            return delegate.getBindings();
        }

        @Override
        public void bind(String name, Object value) {
            delegate.bind(name, value);
        }

        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }

        @Override
        public void appendSql(String sql) {
            // 暂时先放到buffer中，等待处理
            sqlBuffer.append(sql);
        }

        @Override
        public String getSql() {
            return delegate.getSql();
        }

        private void applyPrefix(StringBuilder sql, String trimmedUppercaseSql) {
            if (!prefixApplied) {
                prefixApplied = true;
                // 首先移除prefixesToOverride中的内容
                if (prefixesToOverride != null) {
                    // 检测当前 sql 字符串是否包含 toRemove 前缀，比如 'AND ', 'AND\t'
                    for (String toRemove : prefixesToOverride) {
                        // 只匹配一个
                        if (trimmedUppercaseSql.startsWith(toRemove)) {
                            sql.delete(0, toRemove.trim().length());
                            break;
                        }
                    }
                }
                // 然后添加前缀，比如WHERE
                if (prefix != null) {
                    sql.insert(0, " ");
                    sql.insert(0, prefix);
                }
            }
        }

        private void applySuffix(StringBuilder sql, String trimmedUppercaseSql) {
            if (!suffixApplied) {
                suffixApplied = true;
                // 首先移除suffixesToOverride中的内容
                if (suffixesToOverride != null) {
                    for (String toRemove : suffixesToOverride) {
                        if (trimmedUppercaseSql.endsWith(toRemove) || trimmedUppercaseSql.endsWith(toRemove.trim())) {
                            int start = sql.length() - toRemove.trim().length();
                            int end = sql.length();
                            sql.delete(start, end);
                            break;
                        }
                    }
                }
                // 然后添加后缀
                if (suffix != null) {
                    sql.append(" ");
                    sql.append(suffix);
                }
            }
        }

    }

}
