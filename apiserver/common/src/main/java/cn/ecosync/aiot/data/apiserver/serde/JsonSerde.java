package cn.ecosync.aiot.data.apiserver.serde;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.StringUtils;

/**
 * @author yan
 * @since 2024
 */
public class JsonSerde {
    private final ObjectMapper objectMapper;

    public JsonSerde(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String serialize(Object in) {
        try {
            return objectMapper.writeValueAsString(in);
        } catch (Exception e) {
            throw new RuntimeException("Serialization exception", e);
        }
    }

    public <T> T deserialize(String in, Class<T> outType) {
        try {
            if (!StringUtils.hasText(in)) {
                return null;
            }
            return objectMapper.readValue(in, outType);
        } catch (Exception e) {
            throw new RuntimeException("Serialization exception", e);
        }
    }

    public <T> T deserialize(String in, TypeReference<T> outTypeRef) {
        try {
            if (!StringUtils.hasText(in)) {
                return null;
            }
            JavaType valueType = objectMapper.getTypeFactory().constructType(outTypeRef.getType());
            return objectMapper.readValue(in, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Serialization exception", e);
        }
    }

    public <T> T convert(Object in, Class<T> outType) {
        try {
            return objectMapper.convertValue(in, outType);
        } catch (Exception e) {
            throw new RuntimeException("Serialization exception", e);
        }
    }

    public <T> T convert(Object in, TypeReference<T> outTypeRef) {
        try {
            JavaType valueType = objectMapper.getTypeFactory().constructType(outTypeRef.getType());
            return objectMapper.convertValue(in, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Serialization exception", e);
        }
    }
}
