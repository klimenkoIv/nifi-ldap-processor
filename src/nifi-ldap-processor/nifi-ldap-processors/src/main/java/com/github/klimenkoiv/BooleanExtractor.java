package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

class BooleanExtractor implements IAttributeValueExtractor {

    @Override
    public Boolean ExtractValue(Attribute attribute) throws NamingException {

        return (Boolean) attribute.get();
    }
}
