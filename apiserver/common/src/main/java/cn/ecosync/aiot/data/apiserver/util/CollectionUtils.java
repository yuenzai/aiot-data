package cn.ecosync.aiot.data.apiserver.util;

import org.springframework.lang.Nullable;

import java.util.*;
import java.util.function.Function;

/**
 * @author yan
 * @since 2024
 */
public class CollectionUtils extends org.springframework.util.CollectionUtils {
    public static <T> Collection<T> nullSafeOf(@Nullable Collection<T> collection) {
        return isEmpty(collection) ? Collections.emptyList() : collection;
    }

    public static <T> List<T> nullSafeOf(@Nullable List<T> list) {
        return isEmpty(list) ? Collections.emptyList() : list;
    }

    public static <T> Set<T> nullSafeOf(@Nullable Set<T> set) {
        return isEmpty(set) ? Collections.emptySet() : set;
    }

    public static <K, V> Map<K, V> nullSafeOf(@Nullable Map<K, V> map) {
        return isEmpty(map) ? Collections.emptyMap() : map;
    }

    public static boolean notEmpty(@Nullable Collection<?> collection) {
        return !isEmpty(collection);
    }

    public static boolean notEmpty(@Nullable Map<?, ?> map) {
        return !isEmpty(map);
    }

    public static boolean hasUniqueElement(Collection<?> collection) {
        return hasUniqueElement(collection, Function.identity());
    }

    public static <E> boolean hasUniqueElement(Collection<E> collection, Function<E, ?> function) {
        Set<Object> set = CollectionUtils.newHashSet(collection.size());
        for (E elem : collection) {
            if (!set.add(function.apply(elem))) {
                return false;
            }
        }
        return true;
    }
}
