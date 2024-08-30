package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.text.ParseException;

class SidExtractor implements IAttributeValueExtractor {

    private String covertToHex(byte b) {
        String result = Integer.toHexString((int) b & 0xFF);
        if (result.length() < 2) {
            result = "0" + result;
        }
        return result;
    }

    @Override
    public String ExtractValue(Attribute attribute) throws NamingException, ParseException {
        byte[] sid_array = (byte[]) attribute.get();
        String strSID = "";
        int version;
        long authority;
        int count;
        String rid = "";
        strSID = "S";

        // get version
        version = sid_array[0];
        strSID = strSID + "-" + Integer.toString(version);
        for (int i = 6; i > 0; i--) {
            rid += this.covertToHex(sid_array[i]);
        }

        // get authority
        authority = Long.parseLong(rid);
        strSID = strSID + "-" + authority;

        //next byte is the count of sub-authorities
        count = sid_array[7] & 0xFF;

        //iterate all the sub-auths
        for (int i = 0; i < count; i++) {
            rid = "";
            for (int j = 11; j > 7; j--) {
                rid += this.covertToHex(sid_array[j + (i * 4)]);
            }
            strSID = strSID + "-" + Long.parseLong(rid, 16);
        }
        return strSID;
    }
}
