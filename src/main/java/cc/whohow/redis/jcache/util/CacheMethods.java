package cc.whohow.redis.jcache.util;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import javax.cache.annotation.CacheKey;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CacheMethods {
    private static final TypeFactory TYPE_FACTORY = TypeFactory.defaultInstance();

    public static Type[] getKeyType(Method method) {
        List<Type> result = new ArrayList<>(method.getParameterCount());
        Type[] parameterTypes = method.getGenericParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterTypes.length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            for (Annotation annotation : annotations) {
                if (annotation.annotationType() == CacheKey.class)  {
                    result.add(parameterTypes[i]);
                    break;
                }
            }
        }
        if (result.isEmpty()) {
            return parameterTypes;
        } else {
            return result.toArray(new Type[result.size()]);
        }
    }

    public static String[] getKeyTypeCanonicalName(Method method) {
        return Arrays.stream(getKeyType(method))
                .map(TYPE_FACTORY::constructType)
                .map(JavaType::toCanonical)
                .toArray(String[]::new);
    }

    public static Type getValueType(Method method) {
        return method.getGenericReturnType();
    }

    public static String getValueTypeCanonicalName(Method method) {
        return TYPE_FACTORY.constructType(getValueType(method)).toCanonical();
    }
}
