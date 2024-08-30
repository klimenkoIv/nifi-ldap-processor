package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

class IntExtractor implements IAttributeValueExtractor {

    @Override
    public Integer ExtractValue(Attribute attribute) throws NamingException {
        return (Integer) attribute.get();
    }
}
