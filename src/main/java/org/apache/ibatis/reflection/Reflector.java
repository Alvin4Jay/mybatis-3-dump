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
package org.apache.ibatis.reflection;

import org.apache.ibatis.reflection.invoker.*;
import org.apache.ibatis.reflection.property.PropertyNamer;

import java.lang.reflect.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class represents a cached set of class definition information that
 * allows for easy mapping between property names and getter/setter methods.
 *
 * @author Clinton Begin
 */
public class Reflector {

    private final Class<?> type;
    private final String[] readablePropertyNames;
    private final String[] writablePropertyNames;
    private final Map<String, Invoker> setMethods = new HashMap<>();
    private final Map<String, Invoker> getMethods = new HashMap<>();
    private final Map<String, Class<?>> setTypes = new HashMap<>();
    private final Map<String, Class<?>> getTypes = new HashMap<>();
    private Constructor<?> defaultConstructor;

    private Map<String, String> caseInsensitivePropertyMap = new HashMap<>();

    public Reflector(Class<?> clazz) {
        type = clazz;
        addDefaultConstructor(clazz); // 解析目标类的默认构造方法，并赋值给 defaultConstructor 变量
        addGetMethods(clazz); // 解析 getter 方法，并将解析结果放入 getMethods 中
        addSetMethods(clazz); // 解析 setter 方法，并将解析结果放入 setMethods 中
        addFields(clazz); // 解析属性字段，并将解析结果添加到 setMethods 或 getMethods 中
        readablePropertyNames = getMethods.keySet().toArray(new String[0]); // 从 getMethods 映射中获取可读属性名数组
        writablePropertyNames = setMethods.keySet().toArray(new String[0]); // 从 setMethods 映射中获取可写属性名数组
        // 将所有属性名的大写形式作为键，属性名作为值，存入到 caseInsensitivePropertyMap 中
        for (String propName : readablePropertyNames) {
            caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
        }
        for (String propName : writablePropertyNames) {
            caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
        }
    }

    /**
     * Checks whether can control member accessible.
     *
     * @return If can control member accessible, it return {@literal true}
     * @since 3.5.0
     */
    public static boolean canControlMemberAccessible() {
        try {
            SecurityManager securityManager = System.getSecurityManager();
            if (null != securityManager) {
                securityManager.checkPermission(new ReflectPermission("suppressAccessChecks"));
            }
        } catch (SecurityException e) {
            return false;
        }
        return true;
    }

    private void addDefaultConstructor(Class<?> clazz) {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        Arrays.stream(constructors).filter(constructor -> constructor.getParameterTypes().length == 0)
            .findAny().ifPresent(constructor -> this.defaultConstructor = constructor);
    }

    private void addGetMethods(Class<?> clazz) {
        Map<String, List<Method>> conflictingGetters = new HashMap<>();
        // 获取当前类，接口，以及父类中的方法。
        Method[] methods = getClassMethods(clazz);
        // 过滤出以 get 或 is 开头的方法
        // 将 getXXX 或 isXXX 等方法名转成相应的属性，比如 getName -> name
        Arrays.stream(methods).filter(m -> m.getParameterTypes().length == 0 && PropertyNamer.isGetter(m.getName()))
            /*
             * 将冲突的方法添加到 conflictingGetters 中。考虑这样一种情况：
             *
             * getTitle 和 isTitle 两个方法经过 methodToProperty 处理，
             * 均得到 name = title，这会导致冲突。
             *
             * 对于冲突的方法，这里先统一起存起来，后续再解决冲突
             */
            .forEach(m -> addMethodConflict(conflictingGetters, PropertyNamer.methodToProperty(m.getName()), m));
        // 解决 getter 冲突
        resolveGetterConflicts(conflictingGetters);
    }

    /** 解决 getter 冲突 */
    private void resolveGetterConflicts(Map<String, List<Method>> conflictingGetters) {
        for (Entry<String, List<Method>> entry : conflictingGetters.entrySet()) {
            Method winner = null;
            String propName = entry.getKey();
            boolean isAmbiguous = false;
            for (Method candidate : entry.getValue()) {
                if (winner == null) {
                    winner = candidate;
                    continue;
                }
                // 获取返回值类型
                Class<?> winnerType = winner.getReturnType();
                Class<?> candidateType = candidate.getReturnType();
                /*
                 * 两个方法的返回值类型一致，若两个方法返回值类型均为 boolean，则选取 isXXX 方法
                 * 为 winner。否则无法决定哪个方法更为合适，歧义
                 */
                if (candidateType.equals(winnerType)) {
                    if (!boolean.class.equals(candidateType)) {
                        isAmbiguous = true;
                        break;
                    } else if (candidate.getName().startsWith("is")) {
                        winner = candidate;
                    }
                } else if (candidateType.isAssignableFrom(winnerType)) {
                    /*
                     * winnerType 是 candidateType 的子类，类型上更为具体，
                     * 则认为当前的 winner 仍是合适的，无需做什么事情
                     */
                    // OK getter type is descendant(子孙)
                } else if (winnerType.isAssignableFrom(candidateType)) {
                    /*
                     * candidateType 是 winnerType 的子类，此时认为 candidate 方法更为合适，
                     * 故将 winner 更新为 candidate
                     */
                    winner = candidate;
                } else {
                    isAmbiguous = true;
                    break;
                }
            }
            // 将筛选出的方法添加到 getMethods 中，并将方法返回值添加到 getTypes 中
            addGetMethod(propName, winner, isAmbiguous);
        }
    }

    private void addGetMethod(String name, Method method, boolean isAmbiguous) {
        MethodInvoker invoker = isAmbiguous
            ? new AmbiguousMethodInvoker(method, MessageFormat.format(
            "Illegal overloaded getter method with ambiguous type for property ''{0}'' in class ''{1}''. This breaks the JavaBeans specification and can cause unpredictable results.",
            name, method.getDeclaringClass().getName()))
            : new MethodInvoker(method);
        getMethods.put(name, invoker);
        // 解析返回值类型 TODO 泛型参数解析过程待看
        Type returnType = TypeParameterResolver.resolveReturnType(method, type);
        // 将返回值类型由 Type 转为 Class，并将转换后的结果缓存到 setTypes 中
        getTypes.put(name, typeToClass(returnType));
    }

    private void addSetMethods(Class<?> clazz) {
        Map<String, List<Method>> conflictingSetters = new HashMap<>();
        // 获取当前类，接口，以及父类中的方法。
        Method[] methods = getClassMethods(clazz);
        // 过滤出 setter 方法，且方法仅有一个参数
        Arrays.stream(methods).filter(m -> m.getParameterTypes().length == 1 && PropertyNamer.isSetter(m.getName()))
            /*
             * setter 方法发生冲突原因是：可能存在重载情况，比如：
             *     void setSex(int sex);
             *     void setSex(SexEnum sex);
             */
            .forEach(m -> addMethodConflict(conflictingSetters, PropertyNamer.methodToProperty(m.getName()), m));
        // 解决 setter 冲突
        resolveSetterConflicts(conflictingSetters);
    }

    /** 添加属性名和方法对象到冲突集合中 */
    private void addMethodConflict(Map<String, List<Method>> conflictingMethods, String name, Method method) {
        if (isValidPropertyName(name)) {
            List<Method> list = conflictingMethods.computeIfAbsent(name, k -> new ArrayList<>());
            list.add(method);
        }
    }

    private void resolveSetterConflicts(Map<String, List<Method>> conflictingSetters) {
        for (Entry<String, List<Method>> entry : conflictingSetters.entrySet()) {
            String propName = entry.getKey();
            List<Method> setters = entry.getValue();
            /*
             * 获取 getter 方法的返回值类型，由于 getter 方法不存在重载的情况，
             * 所以可以用它的返回值类型反推哪个 setter 的更为合适
             */
            Class<?> getterType = getTypes.get(propName);
            boolean isGetterAmbiguous = getMethods.get(propName) instanceof AmbiguousMethodInvoker;
            boolean isSetterAmbiguous = false;
            Method match = null;
            for (Method setter : setters) {
                if (!isGetterAmbiguous && setter.getParameterTypes()[0].equals(getterType)) {
                    // should be the best match 参数类型和返回类型一致，则认为是最好的选择，并结束循环
                    match = setter;
                    break;
                }
                if (!isSetterAmbiguous) { // 选择一个更为合适的方法
                    match = pickBetterSetter(match, setter, propName);
                    isSetterAmbiguous = match == null;
                }
            }
            if (match != null) {
                // 将筛选出的方法放入 setMethods 中，并将方法参数值添加到 setTypes 中
                addSetMethod(propName, match);
            }
        }
    }

    /** 从两个 setter 方法中选择一个更为合适方法 */
    private Method pickBetterSetter(Method setter1, Method setter2, String property) {
        if (setter1 == null) {
            return setter2;
        }
        Class<?> paramType1 = setter1.getParameterTypes()[0];
        Class<?> paramType2 = setter2.getParameterTypes()[0];
        // 如果参数2可赋值给参数1，即参数2是参数1的子类，则认为参数2对应的 setter 方法更为合适
        if (paramType1.isAssignableFrom(paramType2)) {
            return setter2;
        } else if (paramType2.isAssignableFrom(paramType1)) {
            return setter1;
        }
        // 两种参数类型不相关，这里抛出异常，歧义
        MethodInvoker invoker = new AmbiguousMethodInvoker(setter1,
            MessageFormat.format(
                "Ambiguous setters defined for property ''{0}'' in class ''{1}'' with types ''{2}'' and ''{3}''.",
                property, setter2.getDeclaringClass().getName(), paramType1.getName(), paramType2.getName()));
        setMethods.put(property, invoker);
        // 解析参数类型列表
        Type[] paramTypes = TypeParameterResolver.resolveParamTypes(setter1, type);
        // 将参数类型由 Type 转为 Class，并将转换后的结果缓存到 setTypes
        setTypes.put(property, typeToClass(paramTypes[0]));
        return null;
    }

    private void addSetMethod(String name, Method method) {
        MethodInvoker invoker = new MethodInvoker(method);
        setMethods.put(name, invoker);
        Type[] paramTypes = TypeParameterResolver.resolveParamTypes(method, type);
        setTypes.put(name, typeToClass(paramTypes[0]));
    }

    private Class<?> typeToClass(Type src) {
        Class<?> result = null;
        if (src instanceof Class) {
            result = (Class<?>) src;
        } else if (src instanceof ParameterizedType) {
            result = (Class<?>) ((ParameterizedType) src).getRawType();
        } else if (src instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType) src).getGenericComponentType();
            if (componentType instanceof Class) {
                result = Array.newInstance((Class<?>) componentType, 0).getClass();
            } else {
                Class<?> componentClass = typeToClass(componentType);
                result = Array.newInstance(componentClass, 0).getClass();
            }
        }
        if (result == null) {
            result = Object.class;
        }
        return result;
    }

    private void addFields(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (!setMethods.containsKey(field.getName())) {
                // issue #379 - removed the check for final because JDK 1.5 allows
                // modification of final fields through reflection (JSR-133). (JGB)
                // pr #16 - final static can only be set by the classloader
                int modifiers = field.getModifiers();
                if (!(Modifier.isFinal(modifiers) && Modifier.isStatic(modifiers))) {
                    addSetField(field);
                }
            }
            if (!getMethods.containsKey(field.getName())) {
                addGetField(field);
            }
        }
        if (clazz.getSuperclass() != null) {
            addFields(clazz.getSuperclass());
        }
    }

    private void addSetField(Field field) {
        if (isValidPropertyName(field.getName())) {
            setMethods.put(field.getName(), new SetFieldInvoker(field));
            Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
            setTypes.put(field.getName(), typeToClass(fieldType));
        }
    }

    private void addGetField(Field field) {
        if (isValidPropertyName(field.getName())) {
            getMethods.put(field.getName(), new GetFieldInvoker(field));
            Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
            getTypes.put(field.getName(), typeToClass(fieldType));
        }
    }

    private boolean isValidPropertyName(String name) {
        return !(name.startsWith("$") || "serialVersionUID".equals(name) || "class".equals(name));
    }

    /**
     * This method returns an array containing all methods
     * declared in this class and any superclass.
     * We use this method, instead of the simpler <code>Class.getMethods()</code>,
     * because we want to look for private methods as well.
     *
     * @param clazz The class
     * @return An array containing all methods in this class
     */
    private Method[] getClassMethods(Class<?> clazz) {
        Map<String, Method> uniqueMethods = new HashMap<>();
        Class<?> currentClass = clazz;
        while (currentClass != null && currentClass != Object.class) {
            addUniqueMethods(uniqueMethods, currentClass.getDeclaredMethods());

            // we also need to look for interface methods -
            // because the class may be abstract
            Class<?>[] interfaces = currentClass.getInterfaces();
            for (Class<?> anInterface : interfaces) {
                addUniqueMethods(uniqueMethods, anInterface.getMethods());
            }

            currentClass = currentClass.getSuperclass();
        }

        Collection<Method> methods = uniqueMethods.values();

        return methods.toArray(new Method[0]);
    }

    private void addUniqueMethods(Map<String, Method> uniqueMethods, Method[] methods) {
        for (Method currentMethod : methods) {
            if (!currentMethod.isBridge()) {
                String signature = getSignature(currentMethod);
                // check to see if the method is already known
                // if it is known, then an extended class must have
                // overridden a method
                if (!uniqueMethods.containsKey(signature)) {
                    uniqueMethods.put(signature, currentMethod);
                }
            }
        }
    }

    private String getSignature(Method method) {
        StringBuilder sb = new StringBuilder();
        Class<?> returnType = method.getReturnType();
        if (returnType != null) {
            sb.append(returnType.getName()).append('#');
        }
        sb.append(method.getName());
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            sb.append(i == 0 ? ':' : ',').append(parameters[i].getName());
        }
        return sb.toString();
    }

    /**
     * Gets the name of the class the instance provides information for.
     *
     * @return The class name
     */
    public Class<?> getType() {
        return type;
    }

    public Constructor<?> getDefaultConstructor() {
        if (defaultConstructor != null) {
            return defaultConstructor;
        } else {
            throw new ReflectionException("There is no default constructor for " + type);
        }
    }

    public boolean hasDefaultConstructor() {
        return defaultConstructor != null;
    }

    public Invoker getSetInvoker(String propertyName) {
        Invoker method = setMethods.get(propertyName);
        if (method == null) {
            throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
        }
        return method;
    }

    public Invoker getGetInvoker(String propertyName) {
        Invoker method = getMethods.get(propertyName);
        if (method == null) {
            throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
        }
        return method;
    }

    /**
     * Gets the type for a property setter.
     *
     * @param propertyName - the name of the property
     * @return The Class of the property setter
     */
    public Class<?> getSetterType(String propertyName) {
        Class<?> clazz = setTypes.get(propertyName);
        if (clazz == null) {
            throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
        }
        return clazz;
    }

    /**
     * Gets the type for a property getter.
     *
     * @param propertyName - the name of the property
     * @return The Class of the property getter
     */
    public Class<?> getGetterType(String propertyName) {
        Class<?> clazz = getTypes.get(propertyName);
        if (clazz == null) {
            throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
        }
        return clazz;
    }

    /**
     * Gets an array of the readable properties for an object.
     *
     * @return The array
     */
    public String[] getGetablePropertyNames() {
        return readablePropertyNames;
    }

    /**
     * Gets an array of the writable properties for an object.
     *
     * @return The array
     */
    public String[] getSetablePropertyNames() {
        return writablePropertyNames;
    }

    /**
     * Check to see if a class has a writable property by name.
     *
     * @param propertyName - the name of the property to check
     * @return True if the object has a writable property by the name
     */
    public boolean hasSetter(String propertyName) {
        return setMethods.containsKey(propertyName);
    }

    /**
     * Check to see if a class has a readable property by name.
     *
     * @param propertyName - the name of the property to check
     * @return True if the object has a readable property by the name
     */
    public boolean hasGetter(String propertyName) {
        return getMethods.containsKey(propertyName);
    }

    public String findPropertyName(String name) {
        return caseInsensitivePropertyMap.get(name.toUpperCase(Locale.ENGLISH));
    }
}
