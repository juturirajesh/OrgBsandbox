/**
 * SCGOEAWSDataSync
 * Pulls person delta files from S3 (s3://salesforce-cgoe-dl/aws-to-crm/) through the Named Credential
 * "AWS_S3_CGOE" and upserts them into Person Accounts on Account.AWS_Person_Id__c (External ID, unique):
 * no Account with that id -> insert, one Account -> update.
 * Email fallback (18 Nov 2026): when no Account has the incoming AWS Person Id, the row's email is checked against
 * Person Accounts that have NO AWS Person Id (people created in Salesforce by staff or by the data migration).
 * Exactly one match -> that Account is updated with the AWS Person Id and every mapped field, so the next run
 * matches it on the External ID. No match -> insert as before. Several matches, or an email used by more than one
 * row in the same chunk (a shared office address) -> not linked, inserted as a new person and counted as a warning.
 * Duplicate rules: the rules on Person Account are set to Allow (alert + report). Apex DML still fails on an Alert
 * rule unless the save sets DuplicateRuleHeader.allowSave, so the sync sets it (ALLOW_DUPLICATE_SAVE); the rule
 * still reports the duplicate.
 * Mapping: "CGOE AWS Mapping Sheet - Internal" - only fields whose CRM field exists are mapped.
 *
 * WHY CHUNKING: a file cannot simply be downloaded and parsed in one go once it gets large.
 *  - The sample feed (aws-to-crm/person_records.jsonl, 15 Oct 2026) is 5,610 bytes for 5 rows = ~1.1 KB per row
 *    (the .json array version is ~1.4 KB per row).
 *  - Async Apex allows ~12 MB of heap and a ~12 MB callout response. The raw text, the split lines and the parsed
 *    records are all in memory at once (~3x the file size), so a single read fails at roughly 3,000-5,000 rows,
 *    i.e. far below the 800,000 people AWS holds.
 *  - One transaction also allows only 10,000 DML rows, and the upsert fires Person Account automation (flows,
 *    duplicate rules), so CPU time grows with every row in the transaction.
 * So .jsonl files of any size are read in byte ranges (HTTP Range, default ~500 KB = ~450 rows per batch chunk),
 * cut on line breaks; each chunk is its own transaction with fresh limits, so heap, callout size and DML rows stay
 * flat however large the file is. Lower the chunk with withChunkBytes() if record automation makes it CPU-heavy.
 * A .json array file cannot be split, so it is read whole and limited to MAX_JSON_BYTES; larger deltas must be JSONL.
 *
 * Load rules:
 *  - Blank source values never overwrite CRM data (a field is only set when the feed has a usable value).
 *  - An invalid email / country / state drops that field only and is reported; the person is still loaded.
 *  - Rows without person_id or last name are skipped and reported with the row's person_id (or, when person_id
 *    itself is blank, its person_num), so the person can be traced back in AWS.
 *  - Address values (street, city, postal code) longer than the target field are cut to the field's length (read
 *    from the org's describe, so a length change in Setup is picked up without a code change) and counted as a
 *    warning per field; the person is still loaded. Other fields are not cut: an over-long value fails that row
 *    with STRING_TOO_LONG. AWS_Person_Id__c is never cut: a shortened key could match a different person.
 *
 * Usage (Anonymous Apex), always scope size 1:
 *   Database.executeBatch(new SCGOEAWSDataSync(null, null, true), 1);   // DRY RUN: upsert + rollback, nothing saved
 *   Database.executeBatch(new SCGOEAWSDataSync(null, null, false), 1);  // all files in the folder
 *   Database.executeBatch(new SCGOEAWSDataSync(new List<String>{'aws-to-crm/person_records.jsonl'}, null, false), 1);
 *   Database.executeBatch(new SCGOEAWSDataSync(), 1);                    // files modified in the last 25 hours
 *   Database.executeBatch(new SCGOEAWSDataSync(null, null, false).withChunkBytes(250000), 1); // smaller chunks
 * Schedule with SCGOEAWSDataSyncScheduler (keeps this class deployable while the job is scheduled).
 *
 * Runs without sharing / SYSTEM_MODE: an integration sync must see every record, otherwise a person the running
 * user cannot see would be inserted again and fail on the unique External ID.
 */
public without sharing class SCGOEAWSDataSync implements Database.Batchable<String>, Database.Stateful,
        Database.AllowsCallouts {

    public static final String NAMED_CREDENTIAL  = 'AWS_S3_CGOE';
    public static final String PREFIX            = 'aws-to-crm/';
    public static final String EXTERNAL_ID       = 'AWS_Person_Id__c';
    public static final String PERSON_RT         = 'PersonAccount';
    public static final Integer DELTA_HOURS      = 25;
    public static final Integer CHUNK_BYTES      = 500000;   // ~450 rows at ~1.1 KB per JSONL row
    public static final Integer OVERLAP_BYTES    = 65536;    // max length of one row
    public static final Integer MAX_JSON_BYTES   = 1000000;  // .json array files are read whole
    public static final Integer MAX_ERRORS_KEPT  = 200;
    public static final Integer MAX_WARNING_KEYS = 100;
    public static final Integer DETAILS_MAX      = 131072;    // Long Text Area length of Error_Log__c.Error_Message__c
    // true = save even when a duplicate rule set to Allow raises an alert (the rule still reports it)
    public static final Boolean ALLOW_DUPLICATE_SAVE = true;
    @TestVisible
    private static final String WARN_EMAIL_MANY = 'email matches several Person Accounts without AWS Person Id - not linked, inserted as new';
    @TestVisible
    private static final String WARN_EMAIL_SHARED = 'email used by several rows in one chunk - not linked by email';

    // Source country names that do not match the org picklist labels (mapping sheet, row 11).
    // Hong Kong has no entry in the org country list, so it is left blank and reported.
    @TestVisible
    private static final Map<String, String> COUNTRY_CROSSWALK = new Map<String, String>{
        'turkey'                           => 'TR',
        'tanzania, the united republic of' => 'TZ',
        'czech republic'                   => 'CZ',
        'venezuela'                        => 'VE',
        'côte d\'ivoire'                   => 'CI',
        'palestinian territory, occupied'  => 'PS',
        'swaziland'                        => 'SZ'
    };

    private static final Pattern EMAIL_PATTERN = Pattern.compile('^[A-Za-z0-9._%+\'-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');
    private static final String HEX_DIGITS = '0123456789abcdef';
    private static final String BOM = '﻿';   // UTF-8 byte-order mark; written as an escape so editors cannot drop it

    /** Source row. Only the mapped JSON keys are declared; JSON.deserialize ignores the rest. */
    public class PersonRecord {
        public String person_id;
        public String person_num;
        public String person_first_name;
        public String person_last_name;
        public String person_type_code;
        public String pref_email_address;
        public String country;
        public String city;
        public String state_province;
        public String pref_phone_raw;
        public String pref_area_code;
        public String mdl_user_id;
        public String mdl_user_username;
        public String company;
        public String lead_industry;
        public String lead_originalsourcetype;
        public String lead_originalsourceinfo;
        public String lead_emailinvalid;
        public String lead_leadsource;
        public String lead_commentcapture;
        public String lead_certinterest;
        public String lead_lifecyclestatusscpd;
        public String lead_mobilephone;
        public String lead_title;
        public String lead_department;
        public String street1;
        public String street2;
        public String zip_code;
        public string job_title;
    }

    /** One entry of an S3 listing. */
    public class S3Object {
        public String key;
        public Long size;
        public Datetime modified;
    }

    private final List<String> fileKeys;
    private final Datetime modifiedAfter;
    private final Boolean dryRun;
    @TestVisible private Integer chunkBytes = CHUNK_BYTES;
    @TestVisible private Integer overlapBytes = OVERLAP_BYTES;

    // Describe caches (per transaction)
    private static Schema.SObjectField externalIdField;
    private static Id personRecordTypeId;
    private static Map<String, String> countryByLabel;
    private static Set<String> countryCodes;
    private static Map<String, Set<String>> statesByCountry;
    private static Map<String, Integer> fieldLengths = new Map<String, Integer>();

    // Job totals (Database.Stateful - kept small on purpose: counters plus capped message lists)
    @TestVisible private Integer files = 0, chunks = 0, received = 0, skipped = 0, created = 0, updated = 0, failed = 0;
    @TestVisible private Integer linkedByEmail = 0;   // existing Accounts given their AWS Person Id through an email match
    @TestVisible private Map<String, Integer> warnings = new Map<String, Integer>();
    @TestVisible private List<String> errors = new List<String>();
    @TestVisible private Integer errorTotal = 0;   // all errors, including those beyond MAX_ERRORS_KEPT


    // ---------- Constructors ----------
    /** Delta mode, used by the scheduler: files modified in the last DELTA_HOURS. */
    public SCGOEAWSDataSync() {
        this(null, Datetime.now().addHours(-DELTA_HOURS), false);
    }

    /**
     * @param fileKeys      explicit S3 keys; null = list the folder
     * @param modifiedAfter when listing, only files modified after this time; null = every file
     * @param dryRun        true = upsert then roll back (validates without saving)
     */
    public SCGOEAWSDataSync(List<String> fileKeys, Datetime modifiedAfter, Boolean dryRun) {
        this.fileKeys = fileKeys;
        this.modifiedAfter = modifiedAfter;
        this.dryRun = dryRun == true;
    }

    /** Bytes read per batch chunk; lower it if record automation makes ~450 rows per transaction too heavy. */
    public SCGOEAWSDataSync withChunkBytes(Integer bytes) {
        this.chunkBytes = bytes;
        return this;
    }

    // ---------- Batchable ----------
    // Returns one entry per chunk: "start|end|size|key" (byte offsets, end exclusive).
    public Iterable<String> start(Database.BatchableContext bc) {
        if (externalIdField() == null) {
            throw new SCGOESyncException('Account.' + EXTERNAL_ID + ' does not exist in this org.');
        }
        if (personRecordTypeId() == null) {
            throw new SCGOESyncException('Person Account record type "' + PERSON_RT + '" not found.');
        }
        List<S3Object> objects = new List<S3Object>();
        if (fileKeys == null) {
            objects = listObjects(PREFIX, modifiedAfter);
        } else {
            for (String key : fileKeys) {
                S3Object match;
                for (S3Object o : listObjects(key, null)) {
                    if (o.key == key) { match = o; }
                }
                if (match == null) { addError(key + ': file not found'); } else { objects.add(match); }
            }
        }
        return toChunks(objects);
    }

    @TestVisible
    private List<String> toChunks(List<S3Object> objects) {
        List<String> out = new List<String>();
        for (S3Object o : objects) {
            if (o.size == null || o.size == 0) {
                continue;
            }
            Long step = isJsonl(o.key) ? (Long) chunkBytes : o.size;
            for (Long s = 0; s < o.size; s += step) {
                out.add(s + '|' + Math.min(s + step, o.size) + '|' + o.size + '|' + o.key);
            }
        }
        return out;
    }

    public void execute(Database.BatchableContext bc, List<String> scope) {
        // All callouts first: a callout is not allowed after DML (or a savepoint) in the same transaction.
        Map<String, List<PersonRecord>> rowsByLabel = new Map<String, List<PersonRecord>>();
        for (String chunk : scope) {
            List<String> p = chunk.split('\\|', 4);
            Long s = Long.valueOf(p[0]), e = Long.valueOf(p[1]), size = Long.valueOf(p[2]);
            String key = p[3];
            String label = isJsonl(key) ? key + ' [bytes ' + s + '-' + e + ']' : key;
            chunks++;
            if (s == 0) {
                files++;
            }
            try {
                //rowsByLabel.put(label, isJsonl(key)
                //    ? parseLines(readChunk(key, s, e, size), label)
                //    : parseJsonArray(key, size));
                if(isJsonl(key)){
                     rowsByLabel.put(
                    label,
                    parseLines(readChunk(key, s, e, size), label)
                  );
                }
            } catch (Exception ex) {
                addError(label + ': ' + ex.getMessage());
            }
        }
        for (String label : rowsByLabel.keySet()) {
            try {
                upsertAccounts(rowsByLabel.get(label), label);
            } catch (Exception ex) {
                addError(label + ': ' + ex.getTypeName() + ' - ' + ex.getMessage());
            }
        }
    }

    public void finish(Database.BatchableContext bc) {
        AsyncApexJob job = jobInfo(bc.getJobId());
        String summary = 'SCGOE AWS sync' + (dryRun ? ' (DRY RUN - rolled back, nothing saved)' : '')
            + '\nFiles: ' + files + '\nChunks: ' + chunks + '\nRows received: ' + received + '\nSkipped: ' + skipped
            + '\nCreated: ' + created + '\nUpdated: ' + updated + ' (of which linked by email: ' + linkedByEmail + ')'
            + '\nFailed: ' + failed
            + '\n\nWarnings (field dropped, person still loaded):\n' + JSON.serializePretty(warnings)
            + '\n\nErrors' + (errors.size() == MAX_ERRORS_KEPT ? ' (first ' + MAX_ERRORS_KEPT + ')' : '') + ':\n'
            + String.join(errors, '\n');
        System.debug(LoggingLevel.INFO, summary);
         
        saveErrorLog(bc.getJobId(), summary, job);
        // Summary email disabled (15 Oct 2026); re-enable together with notify() below if needed.
        // notify(bc, summary);
    }
    // ---------- Error log (finish) ----------
    private static AsyncApexJob jobInfo(Id jobId) {
        List<AsyncApexJob> jobs = [
            SELECT TotalJobItems, JobItemsProcessed, NumberOfErrors, ExtendedStatus
            FROM AsyncApexJob WHERE Id = :jobId LIMIT 1
        ];
        return jobs.isEmpty() ? null : jobs[0];
    }

    // ---------- 1. List the folder (S3 ListObjectsV2, paginated) ----------
    @TestVisible
    private static List<S3Object> listObjects(String prefix, Datetime modifiedAfter) {
        Map<String, S3Object> byKey = new Map<String, S3Object>();
        String token;
        do {
            String url = 'callout:' + NAMED_CREDENTIAL + '/?list-type=2&prefix=' + prefix.replace(' ', '%20');
            if (token != null) {
                url += '&continuation-token=' + EncodingUtil.urlEncode(token, 'UTF-8');
            }
            token = parseListPage(send(url, null, 60000).getBody(), modifiedAfter, byKey);
        } while (token != null);
        List<String> keys = new List<String>(byKey.keySet());
        keys.sort();
        List<S3Object> out = new List<S3Object>();
        for (String k : keys) {
            out.add(byKey.get(k));
        }
        return out;
    }

    // Adds matching .json/.jsonl objects to `byKey`; returns the next continuation token, or null on the last page.
    @TestVisible
    private static String parseListPage(String xml, Datetime modifiedAfter, Map<String, S3Object> byKey) {
        Dom.Document doc = new Dom.Document();
        doc.load(xml);
        String next;
        Boolean truncated = false;
        for (Dom.XmlNode n : doc.getRootElement().getChildElements()) {
            String name = n.getName();
            if (name == 'IsTruncated') {
                truncated = n.getText().trim().equalsIgnoreCase('true');
            } else if (name == 'NextContinuationToken') {
                next = n.getText().trim();
            } else if (name == 'Contents') {
                S3Object o = new S3Object();
                for (Dom.XmlNode c : n.getChildElements()) {
                    String v = c.getText().trim();
                    if (c.getName() == 'Key') { o.key = v; }
                    else if (c.getName() == 'Size') { o.size = Long.valueOf(v); }
                    else if (c.getName() == 'LastModified') { o.modified = (Datetime) JSON.deserialize('"' + v + '"', Datetime.class); }
                }
                Boolean isData = o.key != null && (isJsonl(o.key) || o.key.toLowerCase().endsWith('.json'));
                Boolean isNew = modifiedAfter == null || o.modified == null || o.modified > modifiedAfter;
                if (isData && isNew) {
                    byKey.put(o.key, o);
                }
            }
        }
        return truncated ? next : null;
    }

    // ---------- 2. Read ----------
    // .json array: read whole (cannot be split).
    @TestVisible
    private List<PersonRecord> parseJsonArray(String key, Long size) {
        if (size > MAX_JSON_BYTES) {
            throw new SCGOESyncException('.json file is ' + size + ' bytes (limit ' + MAX_JSON_BYTES
                + '); large files must be sent as .jsonl');
        }
        String body = send(objectUrl(key), null, 120000).getBodyAsBlob().toString().removeStart(BOM);
        return String.isBlank(body) ? new List<PersonRecord>()
            : (List<PersonRecord>) JSON.deserialize(body, List<PersonRecord>.class);
    }

    /**
     * .jsonl: returns the complete lines that START inside [s, e).
     * Reads bytes [s-1, e+overlap): the byte before s tells whether s starts a new line, and the overlap completes
     * the last line. Boundaries are found on the raw bytes (newline = 0x0a), so a multi-byte UTF-8 character
     * split by the range is never decoded.
     */
    @TestVisible
    private String readChunk(String key, Long s, Long e, Long size) {
        Long fromByte = s == 0 ? 0 : s - 1;
        Long toByte = Math.min(size, e + overlapBytes) - 1;
        String hex = EncodingUtil.convertToHex(
            send(objectUrl(key), 'bytes=' + fromByte + '-' + toByte, 120000).getBodyAsBlob());

        Integer first = 0;
        if (s > 0) {
            Integer nl = nextNewline(hex, 0);
            if (nl < 0) {
                return '';
            }
            first = nl + 1;
        }
        Integer relEnd = (Integer) (e - fromByte);
        if (first >= relEnd) {
            return '';   // no line starts in this chunk
        }
        Integer endNl = nextNewline(hex, relEnd - 1);
        Integer last;   // exclusive
        if (endNl >= 0) {
            last = endNl + 1;
        } else if (toByte == size - 1) {
            last = hex.length() / 2;   // final line of the file has no trailing newline
        } else {
            throw new SCGOESyncException('a row near byte ' + e + ' is longer than ' + overlapBytes + ' bytes');
        }
        return EncodingUtil.convertFromHex(hex.substring(first * 2, last * 2)).toString();
    }

    // Byte index of the first newline at or after `fromByte`, or -1.
    private static Integer nextNewline(String hex, Integer fromByte) {
        Integer i = hex.indexOf('0a', fromByte * 2);
        while (i >= 0 && Math.mod(i, 2) == 1) {
            i = hex.indexOf('0a', i + 1);
        }
        return i < 0 ? -1 : i / 2;
    }

    @TestVisible
    private List<PersonRecord> parseLines(String text, String label) {
        List<PersonRecord> rows = new List<PersonRecord>();
        Integer lineNo = 0;
        for (String line : text.removeStart(BOM).split('\n')) {
            lineNo++;
            if (String.isBlank(line)) {
                continue;
            }
            try {
                rows.add((PersonRecord) JSON.deserialize(line, PersonRecord.class));
            } catch (JSONException ex) {
                skipped++;
                addError(label + ' line ' + lineNo + ': invalid JSON - ' + personIdFromRaw(line));
            }
        }
        return rows;
    }

    private static String objectUrl(String key) {
        return 'callout:' + NAMED_CREDENTIAL + '/' + key.replace(' ', '%20');
    }

    private static HttpResponse send(String endpoint, String range, Integer timeoutMs) {
        HttpRequest req = new HttpRequest();
        req.setEndpoint(endpoint);
        req.setMethod('GET');
        req.setTimeout(timeoutMs);
        if (range != null) {
            req.setHeader('Range', range);
        }
        HttpResponse res = new Http().send(req);
        if (res.getStatusCode() != 200 && res.getStatusCode() != 206) {
            throw new SCGOESyncException('S3 GET failed: ' + res.getStatusCode() + ' ' + res.getStatus());
        }
        return res;
    }

    private static Boolean isJsonl(String key) {
        return key.toLowerCase().endsWith('.jsonl');
    }

    // ---------- 3. Map one row to a Person Account ----------
    // Returns null (and a reason in `problems`) when the row cannot be loaded.
    @TestVisible
    private Account toAccount(PersonRecord p, List<String> problems) {
        String personId = clean(p.person_id);
        String lastName = clean(p.person_last_name);
        if (personId == null) { problems.add('person_id missing'); return null; }
        if (lastName == null) { problems.add('person_last_name missing'); return null; }

        Account a = new Account(RecordTypeId = personRecordTypeId());
        a.put(externalIdField(), personId);
        a.LastName = lastName;

        String firstName = clean(p.person_first_name);
        if (firstName != null) { a.FirstName = firstName; }

        String personnum = clean(p.person_num);
        if (personnum != null) { a.AWS_Person_Number__pc = personnum; }

        String email = clean(p.pref_email_address);
        if (email != null) {
            email = email.toLowerCase();
            if (email.length() <= 80 && EMAIL_PATTERN.matcher(email).matches()) {
                a.PersonEmail = email;
            } else {
                warn('invalid email dropped');
            }
        }

        String city = clean(p.city);
        if (city != null) { a.PersonMailingCity = fit('PersonMailingCity', city); }

        String country = clean(p.country);
        String countryCode = countryCode(country);
        if (countryCode != null) {
            a.PersonMailingCountryCode = countryCode;
        } else if (country != null) {
            warn('country not in org picklist: ' + country);
        }

        String state = clean(p.state_province);
        String stateCode = stateCode(state, countryCode);
        if (stateCode != null) {
            a.PersonMailingStateCode = stateCode;
        } else if (state != null && !state.equalsIgnoreCase('foreign')) {
            warn('state not valid for country: ' + state + ' / ' + (countryCode == null ? country : countryCode));
        }
        // Rows 11-12: street1 + street2 -> PersonMailingStreet (one field, two lines)
        String street = joinLines(clean(p.street1), clean(p.street2));
        if (street != null) { a.PersonMailingStreet = fit('PersonMailingStreet', street); }

        String postalCode = clean(p.zip_code);
        if (postalCode != null) { a.PersonMailingPostalCode = fit('PersonMailingPostalCode', postalCode); }

        String phone = validPhone(clean(p.pref_phone_raw));
        if(phone == null){
            phone = validPhone(joinPhone(clean(p.pref_area_code), clean(p.pref_phone_raw)));
        }
        if (phone != null) { a.PersonMobilePhone = phone; }

        String company = clean(p.company);
        if (company != null) { a.Company__pc = company; }

        String jobtitle = clean(p.job_title);
        if (jobtitle != null) { a.AWS_PersonTitle__pc = jobtitle; }
        
        return a;
    }

    // ---------- 4. Upsert on the External ID ----------
    @TestVisible
    private void upsertAccounts(List<PersonRecord> rows, String label) {
        received += rows.size();
        Map<String, Account> byPersonId = new Map<String, Account>();
        for (PersonRecord p : rows) {
            List<String> problems = new List<String>();
            Account a = toAccount(p, problems);
            if (a == null) {
                skipped++;
                addError(label + ': skipped - ' + rowId(p) + ' - ' + String.join(problems, '; '));
                continue;
            }
            String personId = ((String) a.get(externalIdField())).toLowerCase();   // External ID is case-insensitive
            if (byPersonId.containsKey(personId)) {
                warn('person_id repeated in one chunk (last row kept)');
            }
            byPersonId.put(personId, a);   // one row per key: duplicates in one upsert call would fail
        }
        if (byPersonId.isEmpty()) {
            return;
        }

        // Step 1: which AWS Person Ids are already in Salesforce -> update those Accounts.
        Map<String, Id> existingIds = existingIdsByPersonId(byPersonId.keySet());
        // Step 2: new AWS Person Id whose email is on exactly one Person Account without one -> update that Account
        // (taken out of byPersonId). Everything else left in byPersonId is new -> insert.
        List<Account> linked = linkByEmail(byPersonId, existingIds.keySet());
        List<Account> toUpdate = new List<Account>();
        List<Account> toInsert = new List<Account>();
        for (String personId : byPersonId.keySet()) {
            Account a = byPersonId.get(personId);
            if (existingIds.containsKey(personId)) {
                a.Id = existingIds.get(personId);
                toUpdate.add(a);
            } else {
                toInsert.add(a);
            }
        }

        // Insert and update (not upsert): only these take DMLOptions, which carry allowSave for duplicate rules.
        Database.DMLOptions options = dmlOptions();
        Savepoint sp = dryRun ? Database.setSavepoint() : null;
        List<Database.SaveResult> updateResults = toUpdate.isEmpty() ? new List<Database.SaveResult>()
            : Database.update(toUpdate, options, AccessLevel.SYSTEM_MODE);
        List<Database.SaveResult> linkResults = linked.isEmpty() ? new List<Database.SaveResult>()
            : Database.update(linked, options, AccessLevel.SYSTEM_MODE);
        List<Database.SaveResult> insertResults = toInsert.isEmpty() ? new List<Database.SaveResult>()
            : Database.insert(toInsert, options, AccessLevel.SYSTEM_MODE);
        if (dryRun) {
            Database.rollback(sp);
        }
        recordResults(updateResults, toUpdate, label, SaveKind.UPDATED);
        recordResults(linkResults, linked, label, SaveKind.LINKED);
        recordResults(insertResults, toInsert, label, SaveKind.CREATED);
    }

    private enum SaveKind { CREATED, UPDATED, LINKED }

    // Account Id per AWS Person Id (lower case) for the ids that already exist. One query per chunk.
    @TestVisible
    private static Map<String, Id> existingIdsByPersonId(Set<String> personIds) {
        Map<String, Id> out = new Map<String, Id>();
        if (personIds.isEmpty()) {
            return out;
        }
        for (Account a : (List<Account>) Database.queryWithBinds(
                'SELECT Id, ' + EXTERNAL_ID + ' FROM Account WHERE ' + EXTERNAL_ID + ' IN :personIds',
                new Map<String, Object>{ 'personIds' => personIds }, AccessLevel.SYSTEM_MODE)) {
            out.put(((String) a.get(EXTERNAL_ID)).toLowerCase(), a.Id);   // External ID is case-insensitive
        }
        return out;
    }

    /**
     * Email fallback for AWS Person Ids that no Account has yet.
     * Only rows whose AWS Person Id is not in knownIds are considered. Their email is looked up on Person Accounts
     * whose AWS Person Id is blank. Exactly one match -> returns an update for that Account (AWS Person Id + every
     * mapped field) and removes the row from byPersonId; otherwise the row stays in byPersonId and is inserted.
     * One query per chunk, whatever its size; no SOQL in loops.
     */
    @TestVisible
    private List<Account> linkByEmail(Map<String, Account> byPersonId, Set<String> knownIds) {
        List<Account> linked = new List<Account>();
        Map<String, Integer> rowsPerEmail = new Map<String, Integer>();
        for (Account a : byPersonId.values()) {
            String email = emailKey(a);
            if (email != null) {
                rowsPerEmail.put(email, rowsPerEmail.containsKey(email) ? rowsPerEmail.get(email) + 1 : 1);
            }
        }
        if (rowsPerEmail.isEmpty()) {
            return linked;
        }

        Set<String> personIds = byPersonId.keySet();
        // Rows with a new AWS Person Id and an email that only this row uses in the chunk
        Map<String, String> personIdByEmail = new Map<String, String>();
        for (String personId : personIds) {
            String email = emailKey(byPersonId.get(personId));
            if (email == null || knownIds.contains(personId)) {
                continue;
            }
            if (rowsPerEmail.get(email) > 1) {
                warn(WARN_EMAIL_SHARED);
                continue;
            }
            personIdByEmail.put(email, personId);
        }
        if (personIdByEmail.isEmpty()) {
            return linked;
        }

        // Person Accounts without an AWS Person Id holding those emails (SOQL = is case-insensitive)
        Set<String> emails = personIdByEmail.keySet();
        Map<String, List<Id>> holdersByEmail = new Map<String, List<Id>>();
        for (Account h : (List<Account>) Database.queryWithBinds(
                'SELECT Id, PersonEmail FROM Account WHERE IsPersonAccount = true AND ' + EXTERNAL_ID
                + ' = null AND PersonEmail IN :emails',
                new Map<String, Object>{ 'emails' => emails }, AccessLevel.SYSTEM_MODE)) {
            String email = h.PersonEmail.toLowerCase();
            if (!holdersByEmail.containsKey(email)) {
                holdersByEmail.put(email, new List<Id>());
            }
            holdersByEmail.get(email).add(h.Id);
        }

        // Link only an unambiguous match
        for (String email : holdersByEmail.keySet()) {
            List<Id> holders = holdersByEmail.get(email);
            String personId = personIdByEmail.get(email);
            if (holders.size() > 1) {
                warn(WARN_EMAIL_MANY);
                continue;
            }
            linked.add(toLinkedUpdate(byPersonId.remove(personId), holders[0]));
        }
        return linked;
    }

    // Copies every mapped value onto an update of the matched Account. The record type is left as it is: the
    // matched Account is already a Person Account, possibly of another Person Account record type.
    private static Account toLinkedUpdate(Account source, Id accountId) {
        Account target = new Account(Id = accountId);
        for (String field : source.getPopulatedFieldsAsMap().keySet()) {
            if (field != 'RecordTypeId') {
                target.put(field, source.get(field));
            }
        }
        return target;
    }

    private static String emailKey(Account a) {
        return a == null || a.PersonEmail == null ? null : a.PersonEmail.toLowerCase();
    }

    // Partial success (one bad row never fails the chunk). Duplicate rules set to Allow still fail an Apex save on
    // their alert unless allowSave is set; the rule still reports the duplicate.
    private static Database.DMLOptions dmlOptions() {
        Database.DMLOptions options = new Database.DMLOptions();
        options.OptAllOrNone = false;
        options.DuplicateRuleHeader.allowSave = ALLOW_DUPLICATE_SAVE;
        options.DuplicateRuleHeader.runAsCurrentUser = true;
        return options;
    }

    @TestVisible
    private void recordResults(List<Database.SaveResult> results, List<Account> accounts, String label, SaveKind kind) {
        for (Integer i = 0; i < results.size(); i++) {
            Database.SaveResult sr = results[i];
            if (sr.isSuccess()) {
                if (kind == SaveKind.CREATED) {
                    created++;
                } else {
                    updated++;
                }
                if (kind == SaveKind.LINKED) {
                    linkedByEmail++;
                }
                continue;
            }
            failed++;
            String msg = label + ': person_id ' + accounts[i].get(externalIdField())
                + (kind == SaveKind.LINKED ? ' (email match, Account ' + accounts[i].Id + ')' : '') + ' -';
            for (Database.Error e : sr.getErrors()) {
                msg += ' ' + e.getStatusCode() + ' ' + e.getMessage() + ' ' + e.getFields() + ';';
            }
            addError(msg);
        }
    }

    // ---------- Transform helpers ----------
    // Trims; blank and the literal "null" (Marketo placeholder) both mean no value.
    @TestVisible
    private static String clean(String s) {
        if (s == null) {
            return null;
        }
        s = s.trim();
        return (s.length() == 0 || s.equalsIgnoreCase('null')) ? null : s;
    }

    /**
     * Cuts an address value to the Account field's length so one long value cannot fail the whole row with
     * STRING_TOO_LONG. Used for address fields only (street, city, postal code). The length comes from the
     * org's describe (cached per transaction). Each cut is counted as a warning per field: the person is still loaded.
     */
    @TestVisible
    private String fit(String fieldName, String value) {
        if (value == null) {
            return null;
        }
        Integer max = fieldLength(fieldName);
        if (max == null || max <= 0 || value.length() <= max) {
            return value;
        }
        warn(fieldName + ' trimmed to ' + max + ' chars');
        return value.left(max).trim();
    }

    // Describe length of an Account field; null when the field does not exist.
    @TestVisible
    private static Integer fieldLength(String fieldName) {
        if (!fieldLengths.containsKey(fieldName)) {
            Schema.SObjectField f = Schema.SObjectType.Account.fields.getMap().get(fieldName);
            fieldLengths.put(fieldName, f == null ? null : f.getDescribe().getLength());
        }
        return fieldLengths.get(fieldName);
    }

    /** How a skipped row is named in the error log: its person_id, else its person_num, else "no person_id". */
    @TestVisible
    private static String rowId(PersonRecord p) {
        String personId = p == null ? null : clean(p.person_id);
        if (personId != null) {
            return 'person_id ' + personId;
        }
        String personNum = p == null ? null : clean(p.person_num);
        return personNum != null ? 'person_id blank (person_num ' + personNum + ')' : 'person_id blank';
    }

    // A line that is not valid JSON cannot be deserialized, so its person_id is read from the raw text.
    private static final Pattern RAW_PERSON_ID = Pattern.compile('"person_id"\\s*:\\s*"?([^",}\\s]+)');

    @TestVisible
    private static String personIdFromRaw(String line) {
        Matcher m = RAW_PERSON_ID.matcher(line == null ? '' : line);
        return m.find() ? 'person_id ' + m.group(1).left(100) : 'person_id not found in row';
    }

    // Sheet row 16: fewer than 6 digits, or the dummy 5555555, is junk.
    @TestVisible
    private static String validPhone(String raw) {
        if (raw == null) {
            return null;
        }
        String digits = raw.replaceAll('[^0-9]', '');
        
        return (digits.length() < 6 || digits == '5555555') ? null : raw.normalizeSpace().left(40);
    }
    @TestVisible
    private static String joinPhone(String area, String raw) {
    if (raw == null) return null;
    return area == null ? raw : area + ' ' + raw;
    }
   
    // Accepts an ISO code, a crosswalk name, or an org picklist label (case-insensitive).
    @TestVisible
    private static String countryCode(String source) {
        if (source == null) {
            return null;
        }
        if (countryByLabel == null) {
            countryByLabel = new Map<String, String>();
            countryCodes = new Set<String>();
            for (Schema.PicklistEntry pe : Account.PersonMailingCountryCode.getDescribe().getPicklistValues()) {
                if (pe.isActive()) {
                    countryByLabel.put(pe.getLabel().toLowerCase(), pe.getValue());
                    countryCodes.add(pe.getValue());
                }
            }
        }
        String k = source.toLowerCase();
        if (countryCodes.contains(source.toUpperCase())) {
            return source.toUpperCase();
        }
        return COUNTRY_CROSSWALK.containsKey(k) ? COUNTRY_CROSSWALK.get(k) : countryByLabel.get(k);
    }

    // Sheet row 13: 'ON - Ontario' -> 'ON'; 'PQ' -> 'QC'; 'Foreign' -> blank.
    // Returned only when the state is valid for the country (state is a dependent picklist).
    @TestVisible
    private static String stateCode(String source, String countryCode) {
        if (source == null || countryCode == null) {
            return null;
        }
        String s = (source.contains(' - ') ? source.substringBefore(' - ') : source).trim().toUpperCase();
        if (s == 'PQ') {
            s = 'QC';
        }
        Set<String> valid = statesByCountry().get(countryCode);
        return valid != null && valid.contains(s) ? s : null;
    }

    // Decodes the dependent-picklist validFor bitmap: country code -> valid state codes.
    private static Map<String, Set<String>> statesByCountry() {
        if (statesByCountry != null) {
            return statesByCountry;
        }
        statesByCountry = new Map<String, Set<String>>();
        List<Schema.PicklistEntry> countries = Account.PersonMailingCountryCode.getDescribe().getPicklistValues();
        for (Schema.PicklistEntry st : Account.PersonMailingStateCode.getDescribe().getPicklistValues()) {
            if (!st.isActive()) {
                continue;
            }
            String validFor = ((ValidFor) JSON.deserialize(JSON.serialize(st), ValidFor.class)).validFor;
            if (String.isBlank(validFor)) {
                continue;
            }
            String bits = EncodingUtil.convertToHex(EncodingUtil.base64Decode(validFor));
            for (Integer i = 0; i < countries.size() && (i / 8) * 2 + 1 < bits.length(); i++) {
                Integer pos = (i / 8) * 2;
                Integer b = HEX_DIGITS.indexOf(bits.substring(pos, pos + 1)) * 16
                    + HEX_DIGITS.indexOf(bits.substring(pos + 1, pos + 2));
                if ((b & (128 >> Math.mod(i, 8))) != 0) {
                    String c = countries[i].getValue();
                    if (!statesByCountry.containsKey(c)) {
                        statesByCountry.put(c, new Set<String>());
                    }
                    statesByCountry.get(c).add(st.getValue());
                }
            }
        }
        return statesByCountry;
    }
    @TestVisible
    private static String joinLines(String line1, String line2) {
      if (line1 == null) { return line2; }
      if (line2 == null) { return line1; }
      return line1 + '\n' + line2;
    }

    public class ValidFor {
        public String validFor;
    }

    private static Schema.SObjectField externalIdField() {
        if (externalIdField == null) {
            externalIdField = Schema.SObjectType.Account.fields.getMap().get(EXTERNAL_ID);
        }
        return externalIdField;
    }

    private static Id personRecordTypeId() {
        if (personRecordTypeId == null) {
            Schema.RecordTypeInfo rt = Schema.SObjectType.Account.getRecordTypeInfosByDeveloperName().get(PERSON_RT);
            personRecordTypeId = rt == null ? null : rt.getRecordTypeId();
        }
        return personRecordTypeId;
    }

    // ---------- Reporting ----------
    @TestVisible
    private void warn(String message) {
        if (!warnings.containsKey(message) && warnings.size() >= MAX_WARNING_KEYS) {
            message = 'other warnings';
        }
        warnings.put(message, warnings.containsKey(message) ? warnings.get(message) + 1 : 1);
    }

    @TestVisible
    private void addError(String message) {
        // Count every error (saveErrorLog uses this total), keep only the first MAX_ERRORS_KEPT messages.
        errorTotal++;
        if (errors.size() < MAX_ERRORS_KEPT) {
            errors.add(message);
        }
    }
       @TestVisible
    private void saveErrorLog(Id jobId, String summary, AsyncApexJob job) {
        Integer failedChunks = job == null || job.NumberOfErrors == null ? 0 : job.NumberOfErrors;
        if (errorTotal == 0 && failedChunks == 0 && warnings.isEmpty()) {
            return;   // clean run: no error record
        }
 
        String details = summary;
        if (details.length() > DETAILS_MAX) {
            String note = '\n... truncated (field limit ' + DETAILS_MAX + ' chars)';
            details = details.left(DETAILS_MAX - note.length()) + note;
        }
 
        Error_Log__c  log = new Error_Log__c (
            Job_Id__c      = jobId == null ? null : String.valueOf(jobId),
            Error_Count__c  = errorTotal + failedChunks,
            Error_Message__c      = details
        );
 
        try {
            Database.SaveResult result = Database.insert(log, false, AccessLevel.SYSTEM_MODE);
            if (!result.isSuccess()) {
                System.debug(LoggingLevel.WARN, 'Error log not saved: ' + result.getErrors());
            }
        } catch (Exception ex) {
            System.debug(LoggingLevel.WARN, 'Error log not saved: ' + ex.getMessage());
        }
    }      

    // Emails the summary to the user who started the job. Never fails the job (e.g. sandbox email off).
    // Disabled (15 Oct 2026): no emails are sent; the summary is only written to the debug log in finish().
    /*
    private void notify(Database.BatchableContext bc, String summary) {
        try {
            Messaging.SingleEmailMessage mail = new Messaging.SingleEmailMessage();
            mail.setTargetObjectId(UserInfo.getUserId());
            mail.setSaveAsActivity(false);
            mail.setSubject('SCGOE AWS sync ' + (dryRun ? 'DRY RUN ' : '') + (failed + skipped > 0 ? '- issues found' : '- OK')
                + ' (job ' + bc.getJobId() + ')');
            mail.setPlainTextBody(summary);
            Messaging.sendEmail(new List<Messaging.SingleEmailMessage>{ mail });
        } catch (Exception e) {
            System.debug(LoggingLevel.WARN, 'SCGOE AWS sync summary email not sent: ' + e.getMessage());
        }
    }
    */

    public class SCGOESyncException extends Exception {}
}