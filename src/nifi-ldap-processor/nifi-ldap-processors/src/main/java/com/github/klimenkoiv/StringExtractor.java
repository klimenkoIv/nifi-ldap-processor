package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

class StringExtractor implements IAttributeValueExtractor {

    @Override
    public String ExtractValue(Attribute attribute) throws NamingException {
        return (String) attribute.get();
    }
}
