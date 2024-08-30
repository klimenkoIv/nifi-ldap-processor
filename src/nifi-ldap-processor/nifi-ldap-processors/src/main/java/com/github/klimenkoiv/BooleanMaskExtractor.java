package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;

class BooleanMaskExtractor implements IAttributeValueExtractor {
    private final int mask;
    private Boolean negative = false;

    BooleanMaskExtractor(int mask) {
        this.mask = mask;
    }

    BooleanMaskExtractor(int mask, Boolean negative) {
        this.mask = mask;
        this.negative = negative;
    }

    @Override
    public Boolean ExtractValue(Attribute attribute) throws NamingException {
        String uacStrValue = (String) attribute.get();

        if (uacStrValue != null && uacStrValue.length() > 0) {
            final int intValue = Integer.parseInt(uacStrValue);

            if (negative) {
                return (intValue & mask) != mask;
            } else {
                return (intValue & mask) == mask;
            }
        }

        return false;
    }
}
