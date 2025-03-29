package cn.ecosync.aiot.data;

import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yan
 * @since 2024
 */
public class QueryBus {
    private final Map<Class<?>, QueryHandler<?, ?>> queryHandlers;

    public QueryBus(List<QueryHandler<?, ?>> queryHandlers) {
        this.queryHandlers = queryHandlers.stream()
                .map(in -> new AbstractMap.SimpleImmutableEntry<>(getQueryType(in), in))
                .filter(in -> in.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SuppressWarnings("unchecked")
    public <T extends Query<R>, R> R execute(T query) {
        Assert.notNull(query, "query is null");
        QueryHandler<?, ?> queryHandler = queryHandlers.get(query.getClass());
        Assert.notNull(queryHandler, "query handler not found: " + query.getClass().getCanonicalName());
        Method handleMethod = ReflectionUtils.findMethod(queryHandler.getClass(), "handle", query.getClass());
        Assert.notNull(handleMethod, "handle method not found: " + queryHandler.getClass().getCanonicalName());
        return (R) ReflectionUtils.invokeMethod(handleMethod, queryHandler, query);
    }

    private Class<?> getQueryType(QueryHandler<?, ?> queryHandler) {
        return ResolvableType.forClass(QueryHandler.class, queryHandler.getClass())
                .resolveGeneric(0);
    }
}
