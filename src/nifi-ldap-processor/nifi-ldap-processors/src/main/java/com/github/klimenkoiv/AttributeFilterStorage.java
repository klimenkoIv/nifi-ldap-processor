package com.github.klimenkoiv;

import org.apache.nifi.serialization.record.RecordFieldType;

public class AttributeFilterStorage {
    public final String attributeName;
    public final String attributeKey;
    public final RecordFieldType recordFieldType;
    public final IAttributeValueExtractor extractor;

    public AttributeFilterStorage(String AttributeName, String AttributeKey, RecordFieldType RecordFieldType, IAttributeValueExtractor extractor) {
        this.attributeKey = AttributeKey;
        this.recordFieldType = RecordFieldType;
        this.extractor = extractor;
        this.attributeName = AttributeName;
    }
}
