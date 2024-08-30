package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;


public class DateToStringExctractor implements IAttributeValueExtractor {


    @Override
    public String ExtractValue(Attribute attribute) throws NamingException {
        // 0123-45-67 89:1011:1213
        // yyyyMMddhhmmss.S
        String sDate1 = (String) attribute.get();
        String newDate = sDate1.substring(0, 4) + "-" + sDate1.substring(4, 6) +
                "-" + sDate1.substring(6, 8) + " " + sDate1.substring(8, 10) +
                ":" + sDate1.substring(10, 12) + ":" + sDate1.substring(12, 14);
        return newDate;
    }

}
