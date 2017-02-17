package com.chymeravr.analytics.eventjoin;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Base64;

/**
 * Created by rubbal on 31/1/17.
 */
public class Utils {
    private Utils() {
    }

    public static <T extends TBase> T deserializeBase64Thrift(Class<T> type, String base64EncodedString) throws IllegalAccessException, InstantiationException, TException {
        T object = type.newInstance();
        new TDeserializer().deserialize(object, Base64.getDecoder().decode(base64EncodedString));
        return object;
    }

    public static <T extends TBase> String serializeBase64Thrift(T object) throws IllegalAccessException, InstantiationException, TException {
        return Base64.getEncoder().encodeToString(new TSerializer().serialize(object));
    }

}
