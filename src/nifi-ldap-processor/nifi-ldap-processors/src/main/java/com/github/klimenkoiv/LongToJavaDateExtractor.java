package com.github.klimenkoiv;

import org.apache.commons.lang3.time.DateUtils;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

class LongToJavaDateExtractor implements IAttributeValueExtractor {

    @Override
    public Timestamp ExtractValue(Attribute attribute) throws NamingException, ParseException {
        long fileTime = (Long.parseLong((String) attribute.get()) / 10000L) - +11644473600000L;
        Date inputDate = new Date(fileTime);
        inputDate = DateUtils.addHours(inputDate, -3);

        return new Timestamp(inputDate.getTime());

    }
}
