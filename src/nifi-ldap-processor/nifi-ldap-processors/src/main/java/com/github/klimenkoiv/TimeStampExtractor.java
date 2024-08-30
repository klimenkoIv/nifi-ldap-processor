package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.sql.Timestamp;

class TimeStampExtractor implements IAttributeValueExtractor {
    @Override
    public Timestamp ExtractValue(Attribute attribute) throws NamingException {
        Long tmpValue = Long.parseLong((String) attribute.get());
        return new Timestamp(tmpValue);
    }
}
