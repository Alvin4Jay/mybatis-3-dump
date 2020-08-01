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
package org.apache.ibatis.reflection.property;

import java.util.Iterator;

/**
 * 属性名分词器
 *
 * @author Clinton Begin
 */
public class PropertyTokenizer implements Iterator<PropertyTokenizer> {
    private final String indexedName;
    private final String children;
    private String name;
    private String index;

    public PropertyTokenizer(String fullname) {
        // 检测传入的参数中是否包含字符 '.'
        int delim = fullname.indexOf('.');
        if (delim > -1) {
            /*
             * 以点位为界，进行分割。比如：
             *    fullname = xx.yy.zz
             *
             * 以第一个点为分界符：
             *    name = xx
             *    children = yy.zz
             */
            name = fullname.substring(0, delim);
            children = fullname.substring(delim + 1);
        } else {
            // fullname 中不存在字符 '.'
            name = fullname;
            children = null;
        }
        indexedName = name;
        // 检测传入的参数中是否包含字符 '['
        delim = name.indexOf('['); // 数组 xx[]
        if (delim > -1) {
            /*
             * 获取中括号里的内容，比如：
             *   1. 对于数组或List集合：[] 中的内容为数组下标，
             *      比如 fullname = articles[1]，index = 1
             *   2. 对于Map：[] 中的内容为键，
             *      比如 fullname = xxxMap[keyName]，index = keyName
             *
             * 关于 index 属性的用法，可以参考 BaseWrapper 的 getCollectionValue 方法
             */
            index = name.substring(delim + 1, name.length() - 1);
            // 获取分解符前面的内容，比如 fullname = articles[1]，name = articles
            name = name.substring(0, delim);
        }
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getIndexedName() {
        return indexedName;
    }

    public String getChildren() {
        return children;
    }

    @Override
    public boolean hasNext() {
        return children != null;
    }

    @Override
    public PropertyTokenizer next() {
        // 对 children 进行再次切分，用于解析多重复合属性
        return new PropertyTokenizer(children);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not supported, as it has no meaning in the context of properties.");
    }
}
