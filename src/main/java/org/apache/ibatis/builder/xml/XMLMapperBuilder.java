/**
 * Copyright 2009-2020 the original author or authors.
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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.*;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

    private final XPathParser parser;
    private final MapperBuilderAssistant builderAssistant;
    private final Map<String, XNode> sqlFragments;
    private final String resource;

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments, String namespace) {
        this(reader, configuration, resource, sqlFragments);
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments) {
        this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()),
            configuration, resource, sqlFragments);
    }

    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments, String namespace) {
        this(inputStream, configuration, resource, sqlFragments);
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource,
                            Map<String, XNode> sqlFragments) {
        this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
            configuration, resource, sqlFragments);
    }

    private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource,
                             Map<String, XNode> sqlFragments) {
        super(configuration);
        this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
        this.parser = parser;
        this.sqlFragments = sqlFragments;
        this.resource = resource;
    }

    public void parse() {
        // 判断映射文件是否已被解析
        if (!configuration.isResourceLoaded(resource)) {
            // 解析mapper映射文件
            configurationElement(parser.evalNode("/mapper"));
            // 标记为已解析
            configuration.addLoadedResource(resource);
            // 绑定Mapper
            bindMapperForNamespace();
        }

        // 处理未完成解析的节点
        parsePendingResultMaps();
        parsePendingCacheRefs();
        parsePendingStatements();
    }

    public XNode getSqlFragment(String refid) {
        return sqlFragments.get(refid);
    }

    private void configurationElement(XNode context) {
        try {
            // 获取namespace并设置到MapperBuilderAssistant
            String namespace = context.getStringAttribute("namespace");
            if (namespace == null || namespace.isEmpty()) {
                throw new BuilderException("Mapper's namespace cannot be empty");
            }
            builderAssistant.setCurrentNamespace(namespace);
            // 6类节点解析
            cacheRefElement(context.evalNode("cache-ref"));
            cacheElement(context.evalNode("cache")); // 二级缓存
            parameterMapElement(context.evalNodes("/mapper/parameterMap"));
            resultMapElements(context.evalNodes("/mapper/resultMap"));
            sqlElement(context.evalNodes("/mapper/sql"));
            buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
        } catch (Exception e) {
            throw new BuilderException("Error parsing Mapper XML. The XML location is '"
                + resource + "'. Cause: " + e, e);
        }
    }

    private void buildStatementFromContext(List<XNode> list) {
        if (configuration.getDatabaseId() != null) {
            buildStatementFromContext(list, configuration.getDatabaseId());
        }
        buildStatementFromContext(list, null);
    }

    private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
        for (XNode context : list) {
            // statement解析器
            final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant,
                context, requiredDatabaseId);
            try {
                /*
                 * 解析 Statement 节点，并将解析结果存储到
                 * configuration 的 mappedStatements 集合中
                 */
                statementParser.parseStatementNode();
            } catch (IncompleteElementException e) {
                // 解析失败，将解析器放入 configuration 的 incompleteStatements 集合中
                configuration.addIncompleteStatement(statementParser);
            }
        }
    }

    private void parsePendingResultMaps() {
        Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
        synchronized (incompleteResultMaps) {
            Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().resolve();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // ResultMap is still missing a resource...
                }
            }
        }
    }

    private void parsePendingCacheRefs() {
        // 获取未完成解析的 CacheRefResolver 列表
        Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
        synchronized (incompleteCacheRefs) {
            Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
            while (iter.hasNext()) {
                try {
                    /*
                     * 尝试解析 <cache-ref> 节点，若解析失败，则抛出 IncompleteElementException，
                     * 此时下面的删除操作不会被执行
                     */
                    iter.next().resolveCacheRef();
                    /*
                     * 移除 CacheRefResolver 对象。如果代码能执行到此处，
                     * 表明已成功解析了 <cache-ref> 节点
                     */
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // Cache ref is still missing a resource...
                    /*
                     * 如果再次发生 IncompleteElementException 异常，表明当前映射文件中并没有
                     * <cache-ref> 所引用的缓存。有可能所引用的缓存在后面的映射文件中，所以这里
                     * 不能将解析失败的 CacheRefResolver 从集合中删除
                     */
                }
            }
        }
    }

    private void parsePendingStatements() {
        Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
        synchronized (incompleteStatements) {
            Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().parseStatementNode();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // Statement is still missing a resource...
                }
            }
        }
    }

    private void cacheRefElement(XNode context) {
        if (context != null) {
            // 维护缓存引用关系
            configuration.addCacheRef(builderAssistant.getCurrentNamespace(),
                context.getStringAttribute("namespace"));
            CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant,
                context.getStringAttribute("namespace"));
            try {
                // 解析引用的缓存
                cacheRefResolver.resolveCacheRef();
            } catch (IncompleteElementException e) {
                // 这里对 IncompleteElementException 异常进行捕捉，并将 cacheRefResolver 存入到
                // Configuration 的 incompleteCacheRefs 集合中
                configuration.addIncompleteCacheRef(cacheRefResolver);
            }
        }
    }

    private void cacheElement(XNode context) {
        if (context != null) {
            // 缓存类型
            String type = context.getStringAttribute("type", "PERPETUAL");
            Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
            // 缓存项删除策略
            String eviction = context.getStringAttribute("eviction", "LRU");
            Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
            // 缓存清空间隔
            Long flushInterval = context.getLongAttribute("flushInterval");
            // 缓存大小
            Integer size = context.getIntAttribute("size");
            // 是否可以读写
            boolean readWrite = !context.getBooleanAttribute("readOnly", false);
            boolean blocking = context.getBooleanAttribute("blocking", false);
            // 属性
            Properties props = context.getChildrenAsProperties();
            // 构建缓存对象
            builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
        }
    }

    private void parameterMapElement(List<XNode> list) {
        for (XNode parameterMapNode : list) {
            String id = parameterMapNode.getStringAttribute("id");
            String type = parameterMapNode.getStringAttribute("type");
            Class<?> parameterClass = resolveClass(type);
            List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
            List<ParameterMapping> parameterMappings = new ArrayList<>();
            for (XNode parameterNode : parameterNodes) {
                String property = parameterNode.getStringAttribute("property");
                String javaType = parameterNode.getStringAttribute("javaType");
                String jdbcType = parameterNode.getStringAttribute("jdbcType");
                String resultMap = parameterNode.getStringAttribute("resultMap");
                String mode = parameterNode.getStringAttribute("mode");
                String typeHandler = parameterNode.getStringAttribute("typeHandler");
                Integer numericScale = parameterNode.getIntAttribute("numericScale");
                ParameterMode modeEnum = resolveParameterMode(mode);
                Class<?> javaTypeClass = resolveClass(javaType);
                JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
                Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
                ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass,
                    property, javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
                parameterMappings.add(parameterMapping);
            }
            builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
        }
    }

    private void resultMapElements(List<XNode> list) {
        for (XNode resultMapNode : list) {
            try {
                resultMapElement(resultMapNode);
            } catch (IncompleteElementException e) {
                // ignore, it will be retried
            }
        }
    }

    private ResultMap resultMapElement(XNode resultMapNode) {
        return resultMapElement(resultMapNode, Collections.emptyList(), null);
    }

    private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings,
                                       Class<?> enclosingType) {
        ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());

        // 获取type属性
        String type = resultMapNode.getStringAttribute("type",
            resultMapNode.getStringAttribute("ofType",
                resultMapNode.getStringAttribute("resultType",
                    resultMapNode.getStringAttribute("javaType"))));
        // 解析type的实际类型
        Class<?> typeClass = resolveClass(type);
        if (typeClass == null) {
            typeClass = inheritEnclosingType(resultMapNode, enclosingType);
        }
        // 鉴别器
        Discriminator discriminator = null;
        List<ResultMapping> resultMappings = new ArrayList<>(additionalResultMappings);
        List<XNode> resultChildren = resultMapNode.getChildren();
        for (XNode resultChild : resultChildren) {
            if ("constructor".equals(resultChild.getName())) {
                // 解析constructor节点，生成对应的ResultMapping
                processConstructorElement(resultChild, typeClass, resultMappings);
            } else if ("discriminator".equals(resultChild.getName())) {
                // 解析 discriminator 节点
                discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
            } else {
                List<ResultFlag> flags = new ArrayList<>();
                if ("id".equals(resultChild.getName())) {
                    // 添加ID到flags集合中
                    flags.add(ResultFlag.ID);
                }
                // 解析id和result节点，并生成相应的ResultMapping
                resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
            }
        }
        // 获取id、extend、autoMapping，注意id生成方式
        String id = resultMapNode.getStringAttribute("id", resultMapNode.getValueBasedIdentifier());
        String extend = resultMapNode.getStringAttribute("extends");
        Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
        // ResultMap解析器
        ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend,
            discriminator, resultMappings, autoMapping);

        try {
            // 根据前面获取到的信息构建ResultMap对象
            return resultMapResolver.resolve();
        } catch (IncompleteElementException e) {
            // 如果发生IncompleteElementException异常，这里将 resultMapResolver 添加到 incompleteResultMaps 集合中
            configuration.addIncompleteResultMap(resultMapResolver);
            throw e;
        }
    }

    protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {
        if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
            String property = resultMapNode.getStringAttribute("property");
            if (property != null && enclosingType != null) {
                MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
                return metaResultType.getSetterType(property);
            }
        } else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
            return enclosingType;
        }
        return null;
    }

    private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings) {
        // 子节点列表
        List<XNode> argChildren = resultChild.getChildren();
        for (XNode argChild : argChildren) {
            List<ResultFlag> flags = new ArrayList<>();
            flags.add(ResultFlag.CONSTRUCTOR);
            if ("idArg".equals(argChild.getName())) {
                flags.add(ResultFlag.ID);
            }
            resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
        }
    }

    private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType,
                                                      List<ResultMapping> resultMappings) {
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        String typeHandler = context.getStringAttribute("typeHandler");
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Map<String, String> discriminatorMap = new HashMap<>();
        for (XNode caseChild : context.getChildren()) {
            String value = caseChild.getStringAttribute("value");
            String resultMap = caseChild.getStringAttribute("resultMap",
                processNestedResultMappings(caseChild, resultMappings, resultType));
            discriminatorMap.put(value, resultMap);
        }
        return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass,
            discriminatorMap);
    }

    private void sqlElement(List<XNode> list) {
        if (configuration.getDatabaseId() != null) {
            sqlElement(list, configuration.getDatabaseId());
        }
        sqlElement(list, null);
    }

    private void sqlElement(List<XNode> list, String requiredDatabaseId) {
        for (XNode context : list) {
            // 获取databaseId、id
            String databaseId = context.getStringAttribute("databaseId");
            String id = context.getStringAttribute("id");
            // id = currentNamespace + "." + id
            id = builderAssistant.applyCurrentNamespace(id, false);
            // 检测当前 databaseId 和 requiredDatabaseId 是否一致
            if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
                // 将 <id, XNode> 键值对缓存到 sqlFragments 中
                sqlFragments.put(id, context);
            }
        }
    }

    private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
        if (requiredDatabaseId != null) {
            return requiredDatabaseId.equals(databaseId);
        }
        if (databaseId != null) {
            return false;
        }
        if (!this.sqlFragments.containsKey(id)) {
            return true;
        }
        // skip this fragment if there is a previous one with a not null databaseId
        XNode context = this.sqlFragments.get(id);
        return context.getStringAttribute("databaseId") == null;
    }

    private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) {
        String property;
        // 根据节点类型获取name或property属性
        if (flags.contains(ResultFlag.CONSTRUCTOR)) {
            property = context.getStringAttribute("name");
        } else {
            property = context.getStringAttribute("property");
        }
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        String nestedSelect = context.getStringAttribute("select");
        /*
         * 解析resultMap属性，该属性出现在<association>和<collection>节点中。
         * 若这两个节点包含resultMap属性，则调用processNestedResultMappings方法
         * 解析嵌套resultMap。返回ResultMap id
         */
        String nestedResultMap = context.getStringAttribute("resultMap", () ->
            processNestedResultMappings(context, Collections.emptyList(), resultType));
        String notNullColumn = context.getStringAttribute("notNullColumn");
        String columnPrefix = context.getStringAttribute("columnPrefix");
        String typeHandler = context.getStringAttribute("typeHandler");
        String resultSet = context.getStringAttribute("resultSet");
        String foreignColumn = context.getStringAttribute("foreignColumn");
        boolean lazy = "lazy".equals(context.getStringAttribute("fetchType",
            configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
        // 解析javaType、typeHandler的类型以及枚举类型JdbcType
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        // 构建ResultMapping对象
        return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum,
            nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet,
            foreignColumn, lazy);
    }

    /**
     * 嵌套ResultMap解析
     */
    private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings,
                                               Class<?> enclosingType) {
        if (Arrays.asList("association", "collection", "case").contains(context.getName())
            && context.getStringAttribute("select") == null) {
            // <collection/>节点校验
            validateCollection(context, enclosingType);
            ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
            // 返回ResultMap id
            return resultMap.getId();
        }
        return null;
    }

    /**
     * <collection/>节点校验
     */
    protected void validateCollection(XNode context, Class<?> enclosingType) {
        if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
            && context.getStringAttribute("javaType") == null) {
            MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
            String property = context.getStringAttribute("property");
            if (!metaResultType.hasSetter(property)) {
                throw new BuilderException("Ambiguous collection type for property '" + property
                    + "'. You must specify 'javaType' or 'resultMap'.");
            }
        }
    }

    private void bindMapperForNamespace() {
        // 获取映射文件的命名空间
        String namespace = builderAssistant.getCurrentNamespace();
        if (namespace != null) {
            Class<?> boundType = null;
            try {
                // 绑定类型
                boundType = Resources.classForName(namespace);
            } catch (ClassNotFoundException e) {
                // ignore, bound type is not required
            }
            if (boundType != null && !configuration.hasMapper(boundType)) {
                // Spring may not know the real resource name so we set a flag
                // to prevent loading again this resource from the mapper interface
                // look at MapperAnnotationBuilder#loadXmlResource
                configuration.addLoadedResource("namespace:" + namespace);
                // 帮定Mapper
                configuration.addMapper(boundType);
            }
        }
    }

}
