package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

class BooleanCompareExtractor implements IAttributeValueExtractor {
    final Integer data;

    public BooleanCompareExtractor(int i) {
        data = i;
    }

    @Override
    public Object ExtractValue(Attribute attribute) throws NamingException {
        return data == (int) attribute.get();
    }
}
