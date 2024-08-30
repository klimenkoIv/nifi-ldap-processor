package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

class TimeStampFormatExtractor implements IAttributeValueExtractor {
    private final String mask;

    TimeStampFormatExtractor(String maskToParce) {
        this.mask = maskToParce;
    }

    @Override
    public Timestamp ExtractValue(Attribute attribute) throws NamingException, ParseException {

        String sDate1 = (String) attribute.get();
        SimpleDateFormat date1 = new SimpleDateFormat(mask);
        date1.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"));
        Date dt = date1.parse(sDate1);
        //dt = DateUtils.addHours(dt,);
        Timestamp ts = new Timestamp(dt.getTime());
        return ts;
    }
}
