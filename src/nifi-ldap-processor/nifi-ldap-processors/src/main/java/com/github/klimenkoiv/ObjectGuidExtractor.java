package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.nio.ByteBuffer;
import java.util.UUID;

class ObjectGuidExtractor implements IAttributeValueExtractor {

    @Override
    public String ExtractValue(Attribute attribute) throws NamingException {

        byte[] bytes = (byte[]) attribute.get();
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(high, low);
        return uuid.toString();
    }
}
