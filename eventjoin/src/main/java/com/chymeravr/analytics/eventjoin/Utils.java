package com.chymeravr.analytics.eventjoin;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

/**
 * Created by rubbal on 31/1/17.
 */
public class Utils {
    private Utils() {
    }

    public static <T extends TBase> T getThriftObject(Class<T> type, byte[] serializedData) throws IllegalAccessException, InstantiationException, TException {
        T object = type.newInstance();
        new TDeserializer().deserialize(object, serializedData);
        return object;
    }
}
