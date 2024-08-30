/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.klimenkoiv;


import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"ldap"})
@CapabilityDescription("Execute query to ldap server")
@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute = "record.count", description = "Count of records received from LDAP server"),
        @WritesAttribute(attribute = "ldap.error", description = "Possible error on query execution")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class LdapQueryProcessor extends AbstractProcessor {

    public static final String FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
    public static final String SIMPLE = "simple";
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(org.apache.nifi.serialization.RecordSetWriterFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor URL_LDAP = new PropertyDescriptor.Builder()
            .name("url_ldap")
            .displayName("URL ldap server")
            .description("URL for LDAP server in format \"ldap://ldapserver:ldapport\", where ldapserver - name of server, ldapport - port of ldap serverice")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("ldap://my_ldap_server:389")
            .addValidator(Validator.VALID)
            .required(true)
            .build();
    static final PropertyDescriptor BIND_USER = new PropertyDescriptor.Builder()
            .name("bind_user")
            .description("Username for connectinon to LDAP. Should be like \"DOMAIN\\user\"")
            .displayName("Domain username")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor ATR_LIST = new PropertyDescriptor.Builder()
            .name("atr_list")
            .description("Comma separated list of Ldap attributes, which you want to load query from server. If this field is empty, all attributes will be returned")
            .displayName("Attribute list")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor LDAP_FILTER = new PropertyDescriptor.Builder()
            .name("ldap_filter")
            .description("Filter for query ldap server")
            .displayName("LDAP Filter")
            .defaultValue("(&(objectCategory=person)(objectClass=user)(sAMAccountName>=a))")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor BIND_PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Domain user password")
            .description("Access credential to access LDAP server")
            .sensitive(true)
            .addValidator(Validator.VALID)
            .required(true)
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();
    private Set<Relationship> relationships;
    /**
     * Temp field to store current Avro schema
     */
    private RecordSchema CurrentSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        // properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(BIND_PASSWORD);
        properties.add(BIND_USER);
        properties.add(URL_LDAP);
        properties.add(ATR_LIST);
        properties.add(LDAP_FILTER);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {

        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(BIND_PASSWORD);
        descriptors.add(BIND_USER);
        descriptors.add(URL_LDAP);

        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
        getLogger().debug("Init completed");
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        getLogger().debug("OnTrigger start. Flowfile Exist");
        this.CurrentSchema = null;

        //1.  Get fields and variables
        // 1.1.
        String urlLdap = context.getProperty(URL_LDAP)
                .evaluateAttributeExpressions(flowFile).getValue();
        getLogger().debug("Got URL_LDAP: " + urlLdap);
        String userLdap = context.getProperty(BIND_USER)
                .evaluateAttributeExpressions(flowFile).getValue();
        getLogger().debug("Got BIND_USER: " + userLdap);
        String userPassword = context.getProperty(BIND_PASSWORD)
                .evaluateAttributeExpressions(flowFile).getValue();
        getLogger().debug("Got BIND_PASSWORD");
        String ldapFilter = context.getProperty(LDAP_FILTER)
                .evaluateAttributeExpressions(flowFile).getValue();
        getLogger().debug("Got LDAP_FILTER: " + ldapFilter);
        String atrrList = context.getProperty(ATR_LIST)
                .evaluateAttributeExpressions(flowFile).getValue().trim();
        getLogger().debug("Got ATR_LIST: " + atrrList);
        List<AttributeFilterStorage> filter = null;

        if (atrrList != null && !atrrList.isEmpty()) {
            filter = fillFilter(atrrList.split(","));
            getLogger().debug("Got filter");
            ArrayList<RecordField> listOfRecordField = new ArrayList<RecordField>();

            filter.forEach((value) -> {

                RecordField tmp = new RecordField(value.attributeName, value.recordFieldType.getDataType(),
                        true);
                listOfRecordField.add(tmp);
            });
            this.CurrentSchema = new SimpleRecordSchema(listOfRecordField);
            getLogger().debug("Got schema");
        }

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        try {
            getLogger().debug("Ready for Ldap query");
            ArrayList<HashMap<String, Object>> queryResult =
                    PerformLdapQuery(urlLdap, userLdap, userPassword, filter, ldapFilter);
            getLogger().debug("Got records from ldap: " + queryResult.size());
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try {

                        final RecordSetWriter writer = writerFactory.createWriter(getLogger(), CurrentSchema, out, originalAttributes);

                        for (int i = 0; i < queryResult.size(); i++) {
                            writer.write(new MapRecord(CurrentSchema, queryResult.get(i)));
                            recordCount.getAndIncrement();
                        }
                        writer.close();
                        getLogger().debug("Writing completed");
                        attributes.put("record.count", String.valueOf(queryResult.size()));
                        attributes.put("mime.type", writer.getMimeType());

                    } catch (SchemaNotFoundException e) {
                        throw new ProcessException(e.getLocalizedMessage(), e);
                    }

                }
            });


        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", flowFile, e);
            // Since we are wrapping the exceptions above there should always be a cause
            // but it's possible it might not have a message. This handles that by logging
            // the name of the class thrown.
            Throwable c = e.getCause();
            if (c != null) {
                session.putAttribute(flowFile, "ldap.error.message",
                        (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile,
                        "ldap.error.message", e.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully extracted {} records forL {}", count, flowFile);
    }

    private ArrayList<HashMap<String, Object>> PerformLdapQuery(String url, String user, String password, List<AttributeFilterStorage> filter, String ldapFilter) throws NamingException, IOException {
        boolean isAllTributes = filter == null;
        ArrayList<HashMap<String, Object>> result = new ArrayList<>();
        HashSet<String> schemaDict = new HashSet<String>();
        ArrayList<RecordField> tmpRecordSet = new ArrayList<>();
        getLogger().debug("Start filling environments");
        Hashtable<String, Comparable> env = new Hashtable<String, Comparable>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, FACTORY);
        env.put(Context.PROVIDER_URL, url);
        getLogger().debug("URL: {}", url);
        env.put(Context.SECURITY_PRINCIPAL, user);
        getLogger().debug("User: {}", user);
        env.put(Context.SECURITY_CREDENTIALS, password);
        getLogger().debug("Password: {}", password);
        env.put(Context.SECURITY_AUTHENTICATION, SIMPLE);
        env.put(Context.REFERRAL, "follow");
        env.put(Context.BATCHSIZE, "1000");
        env.put("java.naming.ldap.version", "3");
        env.put("java.naming.ldap.attributes.binary", "objectGUID objectSID");
        //end.put(Context.SECURITY_PROTOCOL, "ssl")
        getLogger().debug("Environments are ready. Start creation SearchContols");
        SearchControls searchCtl = new SearchControls();
        if (filter != null) {
            ArrayList<String> temp = new ArrayList<String>();
            filter.forEach((value) -> {
                String key = value.attributeKey;
                temp.add(key);
            });
            String[] retAtr = new String[temp.size()];
            retAtr = temp.toArray(retAtr);
            searchCtl.setReturningAttributes(retAtr);
        }

//Specify the search scope
        searchCtl.setSearchScope(SearchControls.SUBTREE_SCOPE);
        getLogger().debug("Start initial LDAP context");
        LdapContext ctx = new InitialLdapContext(env, null);
        //   log.debug('init complete')
// Activate paged results
        int pageSize = 1000; // 20 entries per page
        byte[] cookie = null;
        int total;
//ctx.setRequestControls(new Control[]{
//        new PagedResultsControl(pageSize, Control.CRITICAL)});
//        new javax.naming.ldap.SortControl("sAMAccountname", false)});
        ctx.setRequestControls(new javax.naming.ldap.Control[]
                {new PagedResultsControl(pageSize, Control.CRITICAL)});

        // def index = 0
        //ldapFilter = "(&(objectCategory=person)(objectClass=user)(sAMAccountName>=a))";
        Boolean work = true;
        while (work) {
            // Perform the search (&(objectCategory=person)(objectClass=user)  //(objectClass=*)
            NamingEnumeration<SearchResult> results =
                    ctx.search("", ldapFilter, searchCtl);
            getLogger().debug("Got results from ldap");
            // Iterate over a batch of search results
            while (results != null && results.hasMore()) {
                // Display an entry
                SearchResult entry = results.next();

                HashMap<String, Object> mMap = new HashMap<String, Object>();
                //Select situation - filter exist
                javax.naming.directory.Attributes Attributes = entry.getAttributes();

                if (isAllTributes) {
                    getLogger().debug("All attributes request");
                    NamingEnumeration<String> iDs = Attributes.getIDs();
                    while (iDs.hasMore()) {
                        String tmpId = iDs.next();
                        Attribute atrValue = Attributes.get(tmpId);
                        if (!schemaDict.contains(tmpId)) {
                            RecordField tmpField = new org.apache.nifi.serialization.record.RecordField(tmpId,
                                    org.apache.nifi.serialization.record.RecordFieldType.STRING.getDataType(), true);
                            schemaDict.add(tmpId);
                            tmpRecordSet.add(tmpField);
                        }
                        mMap.put(tmpId, atrValue.get());
                    }
                } else {
                    // Filter exist.
                    // switch key
                    // select attribute by linked key
                    // get value by rule
                    getLogger().debug("Apply filter");
                    filter.forEach((value) -> {
                        getLogger().debug("Got key: {}, attributeKey: {}", value.attributeName, value.attributeKey);

                        Attribute at = Attributes.get(value.attributeKey);

                        if (at != null) {

                            Object tResult = null;
                            try {
                                getLogger().debug("Got attribute: {}", at.get());
                                tResult = value.extractor.ExtractValue(at);
                                getLogger().debug("Extract value: {}", tResult);
                                mMap.put(value.attributeName, tResult);
                            } catch (NamingException e) {
                                e.printStackTrace();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }

                        }
                    });
                }

                result.add(mMap);

            }
            // Examine the paged results control response
            Control[] controls = ctx.getResponseControls();
            if (controls != null) {
                for (Control control : controls) {
                    if (control instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc =
                                (PagedResultsResponseControl) control;
                        total = prrc.getResultSize();
                        cookie = prrc.getCookie();
                    }  // Handle other response controls (if any)

                }
            }

            // Re-activate paged results
            ctx.setRequestControls(new Control[]{
                    new PagedResultsControl(pageSize, cookie, Control.CRITICAL)});
            //work = index < 20
            work = cookie != null;
        }
//while (cookie != null);

// Close the LDAP association
        ctx.close();
        if (tmpRecordSet.size() > 0) {
            //create common schema for all attributes
            this.CurrentSchema = new SimpleRecordSchema(tmpRecordSet);
        }
        return result;
    }

    private List<AttributeFilterStorage> fillFilter(String[] attrIDs) {
        ArrayList<AttributeFilterStorage> result = new ArrayList();
        StringExtractor stringExtractor = new StringExtractor();
        IntExtractor intExtractor = new IntExtractor();
        TimeStampExtractor timeStampExtractor = new TimeStampExtractor();
        BooleanExtractor booleanExtractor = new BooleanExtractor();

        for (String atr : attrIDs) {
            switch (atr.toLowerCase()) {
                case "accountexpires":
                case "accountexpirationdate": {
                    result.add(new AttributeFilterStorage(atr, "accountExpires",
                            RecordFieldType.TIMESTAMP, timeStampExtractor));//converted to local time
                    continue;
                }
                case "lockouttime":
                case "accountlocouttime": {
                    result.add(new AttributeFilterStorage(atr, "lockoutTime",
                            RecordFieldType.TIMESTAMP, timeStampExtractor));
                    continue; //DateTime RW	, converted to local time
                }
                case "accountnotdelegated": {
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(1048576))); //(bit mask 1048576) Boolean	RW
                    continue;
                }

                case "allowreversiblepasswordencryption": {
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(128)));//Boolean	RW(bit mask 128)
                    continue;
                }
                case "badpwdcount":
                case "badlogoncount": {
                    result.add(new AttributeFilterStorage(atr, "badPwdCount",
                            RecordFieldType.INT, intExtractor));
                    continue;
                }
                case "ntsecuritydescriptor":
                case "cannotchangepassword": {
                    result.add(new AttributeFilterStorage(atr, "nTSecurityDescriptor",
                            RecordFieldType.BOOLEAN, booleanExtractor));
                    continue;
                }
                case "canonicalname": {
                    result.add(new AttributeFilterStorage(atr, "canonicalName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "usercertificate":
                case "certificates": {
                    result.add(new AttributeFilterStorage(atr, "userCertificate",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "pwdLastSet": {
                    result.add(new AttributeFilterStorage(atr, "pwdLastSet",
                            RecordFieldType.INT, intExtractor));
                    continue;
                }
                case "changepasswordatlogon": {
                    result.add(new AttributeFilterStorage(atr, "pwdLastSet",
                            RecordFieldType.BOOLEAN, new BooleanCompareExtractor(0)));
                    //Boolean	W  If pwdLastSet = 0
                    continue;
                }
                case "l":
                case "city": {
                    result.add(new AttributeFilterStorage(atr, "l",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
//                case "cn": {
//                    result.add(new AttributeFilterStorage(atr,"cn",
//                            RecordFieldType.STRING, stringExtractor));
//                    continue;
//                }
                case "company": {
                    result.add(new AttributeFilterStorage(atr, "company",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "c":
                case "country": {
                    result.add(new AttributeFilterStorage(atr, "c",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "whencreated":
                case "created": {
                    // result.add(new AttributeFilterStorage(atr,"whenCreated",
                    //         RecordFieldType.TIMESTAMP, new TimeStampFormatExtractor("yyyyMMddhhmmss.S")));
                    result.add(new AttributeFilterStorage(atr, "whenCreated",
                            RecordFieldType.STRING, new DateToStringExctractor()));
                    continue;

                }
                case "isdeleted":
                case "deleted": {
                    result.add(new AttributeFilterStorage(atr, "isDeleted",
                            RecordFieldType.BOOLEAN, booleanExtractor));
                    continue;
                }
                case "displayname": {
                    result.add(new AttributeFilterStorage(atr, "displayName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }

                case "distinguishedname": {
                    result.add(new AttributeFilterStorage(atr, "distinguishedName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }

                case "doesnotrequirepreauth": {
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(4194304)));
                    //Boolean	RW	userAccountControl (bit mask 4194304)
                    continue;
                }
                case "mail":
                case "emailaddress": {
                    result.add(new AttributeFilterStorage(atr, "mail",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "employeeid": {
                    result.add(new AttributeFilterStorage(atr, "employeeID",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "employeenumber": {
                    result.add(new AttributeFilterStorage(atr, "employeeNumber",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "enabled": {
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(2, true)));   //userAccountControl (bit mask not 2)
                    continue;
                }
                case "facsimiletelephonenumber":
                case "fax": {
                    result.add(new AttributeFilterStorage(atr, "facsimileTelephoneNumber",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "givenname": {
                    result.add(new AttributeFilterStorage(atr, "givenName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "homedirectory": {

                    result.add(new AttributeFilterStorage(atr, "homeDirectory",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "homedirrequired": {
                    //Boolean	RW	userAccountControl (bit mask 8)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(8)));
                    continue;
                }
                case "homedrive": {//	String	RW	homeDrive
                    result.add(new AttributeFilterStorage(atr, "homeDrive",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "wwwhomepage":
                case "homepage": {   //                HomePage	String	RW
                    result.add(new AttributeFilterStorage(atr, "wWWHomePage",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "homephone": {//                HomePhone	String	RW	homePhone
                    result.add(new AttributeFilterStorage(atr, "homePhone",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }

                case "badpasswordtime":
                case "lastbadpasswordattemt": {
                    //                LastBadPasswordAttempt	DateTime	R	badPasswordTime, converted to local time
                    result.add(new AttributeFilterStorage(atr, "badPasswordTime",
                            RecordFieldType.TIMESTAMP, timeStampExtractor));
                    continue;
                }
                case "lastknownparent": {

//                LastKnownParent	String (DN)	R	lastKnownParent
                    result.add(new AttributeFilterStorage(atr, "lastKnownParent",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "lastlogondate":
                case "lastlogontimestamp": {
//                LastLogonDate	DateTime	R	lastLogonTimeStamp, converted to local time
                    result.add(new AttributeFilterStorage(atr, "lastLogonTimeStamp",
                            RecordFieldType.TIMESTAMP, new LongToJavaDateExtractor()));
                    continue;
                }
                case "lockedout": {
//                LockedOut	Boolean	RW	msDS-User-Account-Control-Computed (bit mask 16)
                    result.add(new AttributeFilterStorage(atr, "msDS-User-Account-Control-Computed",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(16)));
                    continue;
                }
                case "lastlogon": {
                    result.add(new AttributeFilterStorage(atr, "lastLogon",
                            RecordFieldType.TIMESTAMP, timeStampExtractor));
                    continue;
                }
                case "userworkstations":
                case "logonworkstation": {
//                LogonWorkstations	String	RW	userWorkstations
                    result.add(new AttributeFilterStorage(atr, "userWorkstations",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "mnslogonaccount": {
//                MNSLogonAccount	Boolean	RW	userAccountControl (bit mask 131072)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor((131072))));
                    continue;
                }
                case "mobile":
                case "mobilephone": {
//                MobilePhone	String	RW	mobile
                    result.add(new AttributeFilterStorage(atr, "mobile",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "whenchanged":
                case "modified": {
//                   Modified	DateTime	R	whenChanged
//                    result.add(new AttributeFilterStorage(atr,"whenChanged",
//                            RecordFieldType.TIMESTAMP,
//                            new TimeStampFormatExtractor("yyyyMMddhhmmss.S")));
                    result.add(new AttributeFilterStorage(atr, "whenChanged",
                            RecordFieldType.STRING,
                            new DateToStringExctractor()));

                    continue;
                }
                case "cn":
                case "name": {
//                Name	String	R	cn (Relative Distinguished Name)
                    result.add(new AttributeFilterStorage(atr, "cn",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "objectcategory": {
//                ObjectCategory	String	R	objectCategory
                    result.add(new AttributeFilterStorage(atr, "objectCategory",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "objectclass": {
//                ObjectClass	String	R	objectClass, most specific value
                    result.add(new AttributeFilterStorage(atr, "objectClass",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "objectguid": {
//                ObjectGUID	Guid	R	objectGUID converted to string
                    result.add(new AttributeFilterStorage(atr, "objectGUID",
                            RecordFieldType.STRING, new ObjectGuidExtractor()));
                    continue;
                }
                case "physicaldeliveryofficename":
                case "office": {
//                Office	String	RW	physicalDeliveryOfficeName
                    result.add(new AttributeFilterStorage(atr, "physicalDeliveryOfficeName", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "telephonenumber":
                case "officephone": {
//                OfficePhone	String	RW	telephoneNumber
                    result.add(new AttributeFilterStorage(atr, "telephoneNumber", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "o":
                case "organization": {
//                Organization	String	RW	o
                    result.add(new AttributeFilterStorage(atr, "o", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "middlename":
                case "othername": {
//                OtherName	String	RW	middleName
                    result.add(new AttributeFilterStorage(atr, "middleName", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "passwordexpired": {
//                PasswordExpired	Boolean	RW	msDS-User-Account-Control-Computed (bit mask 8388608) (see Note 1)
                    result.add(new AttributeFilterStorage(atr, "msDS-User-Account-Control-Computed",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(8388608)));
                    continue;
                }
                case "pwdlastset":
                case "passwordlastset": {
//                PasswordLastSet	DateTime	RW	pwdLastSet, local time
                    result.add(new AttributeFilterStorage(atr, "pwdLastSet",
                            RecordFieldType.TIMESTAMP, timeStampExtractor));
                    continue;
                }
                case "passwordneverexpires": {
//                PasswordNeverExpires	Boolean	RW	userAccountControl (bit mask 65536)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(65536)));
                    continue;
                }
                case "passwordnotrequired": {
//                PasswordNotRequired	Boolean	RW	userAccountControl (bit mask 32)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl", RecordFieldType.BOOLEAN
                            , new BooleanMaskExtractor(32)));
                    continue;
                }
                case "postofficebox":
                case "pobox": {
//                POBox	String	RW	postOfficeBox
                    result.add(new AttributeFilterStorage(atr, "postOfficeBox",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "postalcode": {
//                PostalCode	String	RW	postalCode
                    result.add(new AttributeFilterStorage(atr, "postalCode", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "primarygrouptoken":
                case "primarygroup": {
//                PrimaryGroup	String	R	Group with primaryGroupToken
                    result.add(new AttributeFilterStorage(atr, "primaryGroupToken",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "profilepath": {
//                ProfilePath	String	RW	profilePath
                    result.add(new AttributeFilterStorage(atr, "profilePath", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "ntsecuritydescriptior":
                case "protectedfromaccidentaldeletion": {
//                ProtectedFromAccidentalDeletion	Boolean	RW	nTSecurityDescriptor
                    result.add(new AttributeFilterStorage(atr, "nTSecurityDescriptior",
                            RecordFieldType.BOOLEAN, booleanExtractor));
                    continue;
                }
                case "samaccountname": {
//                SamAccountName	String	RW	sAMAccountName
                    result.add(new AttributeFilterStorage(atr, "sAMAccountName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "scriptpath": {
//                ScriptPath	String	RW	scriptPath
                    result.add(new AttributeFilterStorage(atr, "scriptPath",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "serviceprincipalname":
                case "serviceprincipalnames": {

//                ServicePrincipalNames	ADCollection	RW	servicePrincipalName
                    result.add(new AttributeFilterStorage(atr, "servicePrincipalName", RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "objectsid":
                case "sid": {
//                SID	Sid	R	objectSID converted to string
                    result.add(new AttributeFilterStorage(atr, "objectSID", RecordFieldType.STRING, new SidExtractor()));
                    continue;
                }
                case "sidhistory": {

//                SIDHistory	ADCollection	R	sIDHistory
                    result.add(new AttributeFilterStorage(atr, "sIDHistory",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "smartcardlogonrequired": {
//                SmartcardLogonRequired	Boolean	RW	userAccountControl (bit mask 262144)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(262144)));
                    continue;
                }
                case "st":
                case "state": {
//                State	String	RW	st
                    result.add(new AttributeFilterStorage(atr, "st",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "streetaddress": {
//                StreetAddress	String	RW	streetAddress
                    result.add(new AttributeFilterStorage(atr, "streetAddress",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "sn":
                case "surname": {
//                Surname	String	RW	sn
                    result.add(new AttributeFilterStorage(atr, "sn",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "trustedfordelegation": {
//                TrustedForDelegation	Boolean	RW	userAccountControl (bit mask 524288)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(524288)));
                    continue;
                }
                case "trustedtoauthfordelegation": {
//                TrustedToAuthForDelegation	Boolean	RW	userAccountControl (bit mask 16777216)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(16777216)));
                    continue;
                }
                case "usedeskeyonly": {
//                UseDESKeyOnly	Boolean	RW	userAccountControl (bit mask 2097152)
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.BOOLEAN, new BooleanMaskExtractor(2097152)));
                    continue;
                }
                case "userprincipalname": {
//                UserPrincipalName	String	RW	userPrincipalName
                    result.add(new AttributeFilterStorage(atr, "userPrincipalName",
                            RecordFieldType.STRING, stringExtractor));
                    continue;
                }
                case "useraccountcontrol": {
                    result.add(new AttributeFilterStorage(atr, "userAccountControl",
                            RecordFieldType.INT, intExtractor));
                    continue;
                }
                default: {
                    result.add(new AttributeFilterStorage(atr, atr.toLowerCase(), RecordFieldType.STRING, stringExtractor));
                    continue;
                }
            }
        }

        return result;
    }


}

