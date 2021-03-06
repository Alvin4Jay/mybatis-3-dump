/**
 * Copyright 2009-2017 the original author or authors.
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

/**
 * 存储SQL中<if>节点的内容
 *
 * @author Clinton Begin
 */
public class IfSqlNode implements SqlNode {
    private final ExpressionEvaluator evaluator;
    /** test表达式 */
    private final String test;
    /** if节点内的内容 MixedSqlNode */
    private final SqlNode contents;

    public IfSqlNode(SqlNode contents, String test) {
        this.test = test;
        this.contents = contents;
        this.evaluator = new ExpressionEvaluator();
    }

    @Override
    public boolean apply(DynamicContext context) {
        // 通过 OGNL 评估 test 表达式的结果
        if (evaluator.evaluateBoolean(test, context.getBindings())) {
            // 若 test 表达式中的条件成立，则调用内部节点的 apply 方法进行解析
            contents.apply(context);
            return true;
        }
        return false;
    }

}
