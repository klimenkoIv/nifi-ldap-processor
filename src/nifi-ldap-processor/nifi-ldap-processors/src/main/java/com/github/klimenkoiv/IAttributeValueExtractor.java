package com.github.klimenkoiv;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import java.text.ParseException;

interface IAttributeValueExtractor {

    Object ExtractValue(Attribute attribute) throws NamingException, ParseException;
}
