@IsTest
private class SCGOEAWSDataSyncTest {

    private static final String DIR = 'aws-to-crm/';

    /** Fake S3: paginated ListObjectsV2 (2 pages) and GET with HTTP Range support. Unknown keys return 404. */
    private class S3Mock implements HttpCalloutMock {
        private final Map<String, String> files;

        S3Mock(Map<String, String> files) {
            this.files = files;
        }

        public HttpResponse respond(HttpRequest req) {
            HttpResponse res = new HttpResponse();
            String endpoint = req.getEndpoint();
            if (endpoint.contains('list-type=2')) {
                res.setStatusCode(200);
                res.setBody(listing(!endpoint.contains('continuation-token')));
                return res;
            }
            String key = endpoint.substringAfter('callout:AWS_S3_CGOE/').replace('%20', ' ');
            if (!files.containsKey(key)) {
                res.setStatusCode(404);
                res.setStatus('Not Found');
                return res;
            }
            String hex = EncodingUtil.convertToHex(Blob.valueOf(files.get(key)));
            String range = req.getHeader('Range');
            res.setStatusCode(200);
            if (range != null) {
                Integer fromByte = Integer.valueOf(range.substringBetween('bytes=', '-'));
                Integer toByte = Math.min(Integer.valueOf(range.substringAfter('-')), hex.length() / 2 - 1);
                hex = hex.substring(fromByte * 2, (toByte + 1) * 2);
                res.setStatusCode(206);
            }
            res.setBodyAsBlob(EncodingUtil.convertFromHex(hex));
            return res;
        }

        // Page 1: folder marker, a .txt and the first data file (truncated); page 2: the remaining files.
        private String listing(Boolean firstPage) {
            List<String> keys = new List<String>(files.keySet());
            keys.sort();
            String xml = '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">';
            if (firstPage) {
                xml += '<IsTruncated>' + (keys.size() > 1) + '</IsTruncated><NextContinuationToken>tok/1+=</NextContinuationToken>'
                    + entry(DIR, '') + entry(DIR + 'readme.txt', 'x');
                if (!keys.isEmpty()) {
                    xml += entry(keys[0], files.get(keys[0]));
                }
            } else {
                xml += '<IsTruncated>false</IsTruncated>';
                for (Integer i = 1; i < keys.size(); i++) {
                    xml += entry(keys[i], files.get(keys[i]));
                }
            }
            return xml + '</ListBucketResult>';
        }

        private String entry(String key, String body) {
            String modified = key.contains('old') ? '2026-01-01T00:00:00.000Z' : '2026-10-13T18:41:30.000Z';
            return '<Contents><Key>' + key + '</Key><LastModified>' + modified + '</LastModified><Size>'
                + Blob.valueOf(body).size() + '</Size></Contents>';
        }
    }

    private static String row(String id, String lastName) {
        return '{"person_id":"' + id + '","person_last_name":"' + lastName + '","mdl_user_deleted":0}';
    }

    // 10 rows with multi-byte characters, one CRLF line ending and no trailing newline.
    private static String multiByteJsonl() {
        List<String> names = new List<String>{
            'Zoë', 'Müller', '中村', 'Côté', 'Smith',
            'Åström', 'Ng', 'O\'Brien', 'Łódź', 'Dhar'
        };
        List<String> lines = new List<String>();
        for (Integer i = 0; i < names.size(); i++) {
            lines.add(row(String.valueOf(i + 1), names[i]) + (i == 3 ? '\r' : ''));
        }
        return String.join(lines, '\n');
    }

    private static Boolean hasExternalId() {
        return Schema.SObjectType.Account.fields.getMap().containsKey('aws_person_id__c');
    }

    // ---------- Listing ----------
    @IsTest
    static void listingPaginatesAndFiltersByTypeAndDate() {
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{
            DIR + 'a.json' => '[]', DIR + 'b.jsonl' => 'xy', DIR + 'old.jsonl' => 'z'
        }));
        List<SCGOEAWSDataSync.S3Object> all = SCGOEAWSDataSync.listObjects(DIR, null);
        System.assertEquals(3, all.size(), 'folder marker and .txt are ignored');
        System.assertEquals(DIR + 'a.json', all[0].key);
        System.assertEquals(2, all[1].size);

        List<SCGOEAWSDataSync.S3Object> recent = SCGOEAWSDataSync.listObjects(DIR, Datetime.newInstanceGmt(2026, 6, 1));
        System.assertEquals(2, recent.size(), 'old.jsonl is filtered out by date');
    }

    @IsTest
    static void chunksOnlyJsonlFiles() {
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true).withChunkBytes(100);
        List<SCGOEAWSDataSync.S3Object> objects = new List<SCGOEAWSDataSync.S3Object>();
        for (String spec : new List<String>{ 'big.jsonl:250', 'arr.json:250', 'empty.jsonl:0' }) {
            SCGOEAWSDataSync.S3Object o = new SCGOEAWSDataSync.S3Object();
            o.key = DIR + spec.substringBefore(':');
            o.size = Long.valueOf(spec.substringAfter(':'));
            objects.add(o);
        }
        System.assertEquals(new List<String>{
            '0|100|250|' + DIR + 'big.jsonl', '100|200|250|' + DIR + 'big.jsonl', '200|250|250|' + DIR + 'big.jsonl',
            '0|250|250|' + DIR + 'arr.json'
        }, sync.toChunks(objects));
    }

    // ---------- Chunked reads (the large-volume path) ----------
    @IsTest
    static void everyRowIsReadExactlyOnceWhateverTheChunkSize() {
        String body = multiByteJsonl();
        String key = DIR + 'people.jsonl';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => body }));
        Long size = Blob.valueOf(body).size();

        for (Integer chunk : new List<Integer>{ 11, 37, 100000 }) {   // cuts land mid-row and mid-character
            SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true).withChunkBytes(chunk);
            List<String> ids = new List<String>();
            List<String> lastNames = new List<String>();
            for (Long s = 0; s < size; s += chunk) {
                for (SCGOEAWSDataSync.PersonRecord p : sync.parseLines(sync.readChunk(key, s, Math.min(s + chunk, size), size), 'c')) {
                    ids.add(p.person_id);
                    lastNames.add(p.person_last_name);
                }
            }
            System.assertEquals(new List<String>{ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10' }, ids, 'chunk ' + chunk);
            System.assertEquals('中村', lastNames[2], 'multi-byte text decoded intact, chunk ' + chunk);
            System.assertEquals(0, sync.errors.size(), 'chunk ' + chunk);
        }
    }

    @IsTest
    static void rowLongerThanOverlapIsReported() {
        String key = DIR + 'p.jsonl';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => multiByteJsonl() }));
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        sync.overlapBytes = 3;
        try {
            sync.readChunk(key, 0, 10, Blob.valueOf(multiByteJsonl()).size());
            System.assert(false, 'expected an exception');
        } catch (SCGOEAWSDataSync.SCGOESyncException e) {
            System.assert(e.getMessage().contains('longer than 3 bytes'), e.getMessage());
        }
    }

    @IsTest
    static void parseLinesSkipsBadJsonAndBom() {
        // Data: a leading byte-order mark, then one invalid JSON line and one blank line between two good rows.
        // Expected: the BOM is stripped so the first row loads; 2 rows, 1 skipped.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        List<SCGOEAWSDataSync.PersonRecord> rows =
            sync.parseLines('﻿' + row('1', 'A') + '\n{bad json\n\n' + row('2', 'B') + '\n', 'f');
        System.assertEquals(2, rows.size());
        System.assertEquals('1', rows[0].person_id);
        System.assertEquals(1, sync.skipped);
        System.assert(sync.errors[0].contains('line 2: invalid JSON'), sync.errors[0]);
    }

    @IsTest
    static void jsonArrayIsReadWholeAndSizeLimited() {
        String key = DIR + 'people.json';
        String body = '[' + row('1', 'A') + ',' + row('2', 'B') + ']';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => body, DIR + 'empty.json' => ' ' }));
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        System.assertEquals(2, sync.parseJsonArray(key, Blob.valueOf(body).size()).size());
        System.assertEquals(0, sync.parseJsonArray(DIR + 'empty.json', 1).size());
        try {
            sync.parseJsonArray(key, SCGOEAWSDataSync.MAX_JSON_BYTES + 1);
            System.assert(false, 'expected an exception');
        } catch (SCGOEAWSDataSync.SCGOESyncException e) {
            System.assert(e.getMessage().contains('.jsonl'), e.getMessage());
        }
    }

    // ---------- Mapping ----------
    @IsTest
    static void cleansValues() {
        System.assertEquals(null, SCGOEAWSDataSync.clean('  '));
        System.assertEquals(null, SCGOEAWSDataSync.clean('null'));
        System.assertEquals('x', SCGOEAWSDataSync.clean(' x '));
        System.assertEquals(null, SCGOEAWSDataSync.validPhone(null));
        System.assertEquals(null, SCGOEAWSDataSync.validPhone('68271'));
        System.assertEquals(null, SCGOEAWSDataSync.validPhone('555 5555'));
        System.assertEquals('929 2391051', SCGOEAWSDataSync.validPhone(' 929   2391051 '));
    }

    @IsTest
    static void resolvesCountryAndState() {
        System.assertEquals(null, SCGOEAWSDataSync.countryCode(null));
        System.assertEquals('US', SCGOEAWSDataSync.countryCode('us'));
        System.assertEquals('TR', SCGOEAWSDataSync.countryCode('Turkey'));
        System.assertEquals('IN', SCGOEAWSDataSync.countryCode('India'));
        System.assertEquals(null, SCGOEAWSDataSync.countryCode('Atlantis'));
        System.assertEquals('CA', SCGOEAWSDataSync.stateCode('CA', 'US'));
        System.assertEquals('ON', SCGOEAWSDataSync.stateCode('ON - Ontario', 'CA'));
        System.assertEquals('QC', SCGOEAWSDataSync.stateCode('PQ - Province of Quebec', 'CA'));
        System.assertEquals(null, SCGOEAWSDataSync.stateCode('ON', 'US'), 'state must belong to the country');
        System.assertEquals(null, SCGOEAWSDataSync.stateCode('CA', null));
    }

    @IsTest
    static void mapsRowAndNeverBlanksCrmData() {
        if (!hasExternalId()) {
            return;
        }
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        SCGOEAWSDataSync.PersonRecord p = sync.parseLines('{"person_id":"1001","person_first_name":"Ada",'
            + '"person_last_name":"Lovelace","person_type_code":"Student","preferred_email_address":"Ada@Example.com",'
            + '"country":"India","city":"Pune","state_province":null,"pref_phone_raw":"68271","lead_industry":"null"}', 'f')[0];
        Account a = sync.toAccount(p, new List<String>());
        System.assertEquals('1001', a.get('AWS_Person_Id__c'));
        System.assertEquals('Ada', a.FirstName);
        System.assertEquals('Lovelace', a.LastName);
        System.assertEquals('IN', a.PersonMailingCountryCode);
        System.assertEquals('Pune', a.PersonMailingCity);
        Map<String, Object> populated = a.getPopulatedFieldsAsMap();
        System.assert(!populated.containsKey('Phone'), 'junk phone must not be sent (would blank CRM value)');
        System.assert(!populated.containsKey('Industry'), 'literal "null" must not be sent');
        System.assert(!populated.containsKey('PersonMailingStateCode'), 'blank state must not be sent');

        p.person_type_code = 'Alumni';
        p.pref_email_address = 'not-an-email';
        p.country = 'United States';
        p.state_province = 'ON';
        p.pref_phone_raw = '+1 650 555 0100';
        p.lead_industry = 'Education';
        a = sync.toAccount(p, new List<String>());
        System.assertEquals(null, a.PersonEmail);
        System.assertEquals('US', a.PersonMailingCountryCode);
        System.assertEquals(null, a.PersonMailingStateCode);
        System.assertEquals(1, sync.warnings.get('invalid email dropped'));
        System.assertEquals(1, sync.warnings.get('state not valid for country: ON / US'));

        p.country = 'Atlantis';
        sync.toAccount(p, new List<String>());
        System.assertEquals(1, sync.warnings.get('country not in org picklist: Atlantis'));

        // Warnings are counted per reason; toAccount ran twice for this row, so the invalid email counts twice.
        System.assertEquals(2, sync.warnings.get('invalid email dropped'));
    }

    @IsTest
    static void skipsRowsWithoutKeyOrLastName() {
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        List<String> p1 = new List<String>();
        List<String> p2 = new List<String>();
        System.assertEquals(null, sync.toAccount(sync.parseLines('{"person_id":"1"}', 'f')[0], p1));
        System.assertEquals(null, sync.toAccount(sync.parseLines('{"person_last_name":"X"}', 'f')[0], p2));
        System.assertEquals('person_last_name missing', p1[0]);
        System.assertEquals('person_id missing', p2[0]);
    }

    @IsTest
    static void warningKeysAreCapped() {
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        for (Integer i = 0; i < SCGOEAWSDataSync.MAX_WARNING_KEYS + 10; i++) {
            sync.warn('w' + i);
        }
        System.assertEquals(SCGOEAWSDataSync.MAX_WARNING_KEYS + 1, sync.warnings.size());
        System.assertEquals(10, sync.warnings.get('other warnings'));
    }

    // ---------- execute(): counts, failures, skips, duplicates ----------
    @IsTest
    static void executeUpsertsAndReportsEveryOutcome() {
        if (!hasExternalId()) {
            return;
        }
        String key = DIR + 'mixed.jsonl';
        String body = String.join(new List<String>{
            row('2001', 'Good'),
            row('2002', 'Dup'), row('2002', 'DupLast'),          // repeated person_id: last row kept
            row('2'.repeat(300), 'TooLong'),                      // External ID over 255 chars: DML failure
            '{"person_id":"2004"}',                               // no last name: skipped
            '{not json'                                           // invalid line: skipped
        }, '\n');
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => body }));
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        Long size = Blob.valueOf(body).size();

        sync.execute(null, new List<String>{ '0|' + size + '|' + size + '|' + key, '0|10|10|' + DIR + 'missing.jsonl' });

        System.assertEquals(2, sync.files);
        System.assertEquals(5, sync.received);
        System.assertEquals(2, sync.created);
        System.assertEquals(1, sync.failed);
        System.assertEquals(2, sync.skipped);
        System.assertEquals(1, sync.warnings.get('person_id repeated in one chunk (last row kept)'));
        String allErrors = String.join(sync.errors, '\n');
        System.assert(allErrors.contains('missing.jsonl') && allErrors.contains('404'), allErrors);
        System.assert(allErrors.contains('STRING_TOO_LONG') || allErrors.contains('person_id 222'), allErrors);
        List<Account> dup = Database.query('SELECT LastName, IsPersonAccount FROM Account WHERE AWS_Person_Id__c = \'2002\'');
        System.assertEquals('DupLast', dup[0].LastName);
        System.assertEquals(true, dup[0].IsPersonAccount);
    }

    // ---------- Full batch runs (start + execute + finish) ----------
    @IsTest
    static void dryRunValidatesWithoutSaving() {
        if (!hasExternalId()) {
            return;
        }
        String key = DIR + 'people.jsonl';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => row('3001', 'Dry') }));
        Test.startTest();
        Database.executeBatch(new SCGOEAWSDataSync(new List<String>{ key, DIR + 'nope.jsonl' }, null, true), 1);
        Test.stopTest();
        System.assertEquals(0, Database.countQuery('SELECT COUNT() FROM Account WHERE AWS_Person_Id__c = \'3001\''),
            'dry run must roll back');
    }

    @IsTest
    static void realRunCreatesThenUpdatesPersonAccount() {
        if (!hasExternalId()) {
            return;
        }
        Account existing = new Account(
            RecordTypeId = Schema.SObjectType.Account.getRecordTypeInfosByDeveloperName().get('PersonAccount').getRecordTypeId(),
            LastName = 'Old', Phone = '6505550100');
        existing.put('AWS_Person_Id__c', '4001');
        insert existing;

        String key = DIR + 'people.json';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{
            key => '[' + row('4001', 'New') + ',' + row('4002', 'Brand') + ']'
        }));
        Test.startTest();
        Database.executeBatch(new SCGOEAWSDataSync(null, null, false), 1);
        Test.stopTest();

        Account updated = Database.query('SELECT LastName, Phone FROM Account WHERE AWS_Person_Id__c = \'4001\'');
       
    }

    @IsTest
    static void schedulerStartsDeltaRun() {
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>()));
        Test.startTest();
        String jobId = System.schedule('SCGOE AWS sync test', '0 0 2 * * ?', new SCGOEAWSDataSyncScheduler());
        Test.stopTest();
        System.assertNotEquals(null, jobId);
        System.assertNotEquals(null, new SCGOEAWSDataSync(), 'delta-mode constructor');
    }

    // ---------- Error log (saveErrorLog, called from finish) ----------
    // Scenarios: errors collected by the run, failed batch chunks reported by AsyncApexJob,
    // a warnings-only run (record, count 0), a fully clean run (no record), and a summary longer than the field limit.

    /** Builds an in-memory AsyncApexJob (its fields are not writable, so JSON is used). */
    private static AsyncApexJob fakeJob(Integer numberOfErrors) {
        return (AsyncApexJob) JSON.deserialize(
            '{"attributes":{"type":"AsyncApexJob"},"NumberOfErrors":' + numberOfErrors + '}', AsyncApexJob.class);
    }

    // SYSTEM_MODE: Error_Message__c has no field-level security grant, the class also writes it in SYSTEM_MODE.
    private static List<Error_Log__c> errorLogs() {
        return [SELECT Job_Id__c, Error_Count__c, Error_Message__c FROM Error_Log__c WITH SYSTEM_MODE];
    }

    @IsTest
    static void errorLogSavedWhenRunCollectedErrors() {
        // Data: 2 run errors, no job (job == null branch). Expected: one log with count 2 and both messages.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        sync.errors.add('aws-to-crm/a.jsonl: S3 GET failed: 404 Not Found');
        sync.errors.add('aws-to-crm/b.jsonl line 3: invalid JSON');
        sync.errorTotal = 2;

        Test.startTest();
        // saveErrorLog writes the summary as given; finish() puts the errors into it.
        sync.saveErrorLog(null, 'SCGOE AWS sync summary\n\nErrors:\n' + String.join(sync.errors, '\n'), null);
        Test.stopTest();

        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size(), 'one error log record expected');
        System.assertEquals(2, logs[0].Error_Count__c, 'count = collected errors');
        System.assertEquals(null, logs[0].Job_Id__c, 'no job id was supplied');
        System.assert(logs[0].Error_Message__c.startsWith('SCGOE AWS sync summary'), logs[0].Error_Message__c);
        System.assert(logs[0].Error_Message__c.contains('404 Not Found'), logs[0].Error_Message__c);
        System.assert(logs[0].Error_Message__c.contains('line 3: invalid JSON'), logs[0].Error_Message__c);
    }

    @IsTest
    static void addErrorCountsEveryErrorAndWritesLog() {
        // Data: errors raised through addError (the path a real run uses). Expected: errorTotal counts them
        // and saveErrorLog writes a record without the job reporting failed chunks.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);

        Test.startTest();
        sync.addError('aws-to-crm/a.jsonl: S3 GET failed: 404 Not Found');
        sync.addError('aws-to-crm/b.jsonl line 3: invalid JSON');
        sync.saveErrorLog(null, 'summary', fakeJob(0));
        Test.stopTest();

        System.assertEquals(2, sync.errorTotal, 'addError must count each error');
        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size(), 'ordinary errors must produce an error log');
        System.assertEquals(2, logs[0].Error_Count__c);
    }

    @IsTest
    static void errorLogCountsFailedBatchChunks() {
        // Data: 1 run error + a job with 3 failed chunks. Expected: count 4 and the job id stored.
        Id jobId = Id.valueOf('707000000000001AAA');
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        sync.errors.add('chunk error');
        sync.errorTotal = 1;

        Test.startTest();
        sync.saveErrorLog(jobId, 'summary', fakeJob(3));
        Test.stopTest();

        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size());
        System.assertEquals(4, logs[0].Error_Count__c, 'run errors + failed chunks');
        System.assertEquals(String.valueOf(jobId), logs[0].Job_Id__c);
    }

    @IsTest
    static void errorLogSavedForFailedChunksOnly() {
        // Data: no run errors, job reports 2 failed chunks. Expected: a log is still written.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);

        Test.startTest();
        sync.saveErrorLog(null, 'summary', fakeJob(2));
        Test.stopTest();

        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size());
        System.assertEquals(2, logs[0].Error_Count__c);
    }

    @IsTest
    static void cleanRunWritesNoErrorLog() {
        // Data: no errors; job null, job with 0 errors, job with null NumberOfErrors. Expected: no record.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);

        Test.startTest();
        sync.saveErrorLog(null, 'summary', null);
        sync.saveErrorLog(null, 'summary', fakeJob(0));
        sync.saveErrorLog(null, 'summary', new AsyncApexJob());
        Test.stopTest();

        System.assertEquals(0, errorLogs().size(), 'a clean run must not create an error log');
    }

    @IsTest
    static void warningsOnlyRunWritesErrorLog() {
        // Data: no errors, no failed chunks, 2 warnings (counts per reason, as finish() writes them).
        // Expected: one record, Error_Count__c = 0, message carries the warnings.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        sync.warn('invalid email dropped');
        sync.warn('country not in org picklist: Hong Kong');
        String summary = 'SCGOE AWS sync\n\nWarnings (field dropped, person still loaded):\n'
            + JSON.serializePretty(sync.warnings);

        Test.startTest();
        sync.saveErrorLog(Id.valueOf('707000000000001AAA'), summary, fakeJob(0));
        Test.stopTest();

        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size(), 'a run with warnings must create an error log');
        System.assertEquals(0, logs[0].Error_Count__c, 'warnings are not counted as errors');
        System.assert(logs[0].Error_Message__c.contains('invalid email dropped'), logs[0].Error_Message__c);
        System.assert(logs[0].Error_Message__c.contains('Hong Kong'), logs[0].Error_Message__c);
    }

    @IsTest
    static void oversizedDetailsAreTruncatedAndNeverFailTheJob() {
        // Data: summary longer than DETAILS_MAX (= Error_Message__c length, 32,768).
        // Expected: text is cut to the field length, the record saves and ends with the truncation note.
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        sync.errors.add('one error');
        sync.errorTotal = 1;
        String hugeSummary = 'x'.repeat(SCGOEAWSDataSync.DETAILS_MAX + 1000);

        Test.startTest();
        sync.saveErrorLog(null, hugeSummary, null);
        Test.stopTest();

        List<Error_Log__c> logs = errorLogs();
        System.assertEquals(1, logs.size(), 'the truncated log must save');
        System.assertEquals(SCGOEAWSDataSync.DETAILS_MAX, logs[0].Error_Message__c.length(), 'cut to the field length');
        System.assert(logs[0].Error_Message__c.contains('truncated'), 'saved text must carry the truncation note');
    }

    // ---------- Field-length trimming ----------
    // Address values longer than their Account field are cut to the org's describe length and warned per field.
    // Other fields are passed through uncut and raise no trim warning.
    @IsTest
    static void onlyAddressValuesAreTrimmedToFieldLength() {
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        SCGOEAWSDataSync.PersonRecord p = new SCGOEAWSDataSync.PersonRecord();
        p.person_id = '3001';
        p.person_last_name = 'L'.repeat(100);
        p.person_first_name = 'F'.repeat(60);
        p.person_num = 'N'.repeat(120);
        p.city = 'C'.repeat(60);
        p.street1 = 'S'.repeat(200);
        p.street2 = 'T'.repeat(200);
        p.zip_code = '9'.repeat(30);
        p.company = 'Co'.repeat(200);
        p.job_title = 'J'.repeat(300);

        Account a = sync.toAccount(p, new List<String>());

        Map<String, Object> populated = a.getPopulatedFieldsAsMap();
        for (String f : new List<String>{ 'PersonMailingCity', 'PersonMailingStreet', 'PersonMailingPostalCode' }) {
            Integer max = SCGOEAWSDataSync.fieldLength(f);
            System.assert(populated.containsKey(f), f + ' must still be set');
            System.assertEquals(max, ((String) populated.get(f)).length(), f + ' must be cut to ' + max);
            // each cut is reported as a warning for that field
            String reason = f + ' trimmed to ' + max + ' chars';
            System.assert(sync.warnings.containsKey(reason), 'missing warning: ' + reason + ' in ' + sync.warnings);
        }
        Map<String, Integer> uncut = new Map<String, Integer>{ 'LastName' => 100, 'FirstName' => 60,
            'AWS_Person_Number__pc' => 120, 'Company__pc' => 400, 'AWS_PersonTitle__pc' => 300 };
        for (String f : uncut.keySet()) {
            System.assertEquals(uncut.get(f), ((String) populated.get(f)).length(), f + ' must not be cut');
            for (String w : sync.warnings.keySet()) {
                System.assert(!w.startsWith(f + ' trimmed'), 'unexpected warning: ' + w);
            }
        }
        System.assertEquals('3001', a.get('AWS_Person_Id__c'), 'the External ID is never cut');
    }

    // Values within the limit are untouched and raise no warning; null stays null.
    @IsTest
    static void shortValuesAreNotTrimmed() {
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, true);
        System.assertEquals('Short', sync.fit('PersonTitle', 'Short'));
        System.assertEquals(null, sync.fit('PersonTitle', null));
        System.assertEquals('x'.repeat(500), sync.fit('No_Such_Field__c', 'x'.repeat(500)), 'unknown field: unchanged');
        System.assert(sync.warnings.isEmpty(), String.valueOf(sync.warnings));
    }

    // A full row with over-long address values loads instead of failing with STRING_TOO_LONG.
    @IsTest
    static void executeLoadsRowWithOverlongAddressValues() {
        if (!hasExternalId()) {
            return;
        }
        String key = DIR + 'long.jsonl';
        String body = '{"person_id":"3101","person_last_name":"Long","zip_code":"' + '1'.repeat(40)
            + '","city":"' + 'C'.repeat(60) + '","street1":"' + 'S'.repeat(300)
            + '","job_title":"' + 'B'.repeat(150) + '"}';
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => body }));
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        Long size = Blob.valueOf(body).size();

        sync.execute(null, new List<String>{ '0|' + size + '|' + size + '|' + key });

        System.assertEquals(1, sync.created, String.join(sync.errors, '\n'));
        System.assertEquals(0, sync.failed, String.join(sync.errors, '\n'));
    }

    // ---------- person_id on skipped-row errors ----------
    @IsTest
    static void skippedRowErrorsCarryPersonId() {
        if (!hasExternalId()) {
            return;
        }
        String key = DIR + 'skips.jsonl';
        String body = String.join(new List<String>{
            '{"person_id":"4001"}',                                  // no last name
            '{"person_last_name":"NoId","person_num":"N-77"}',       // no person_id, has person_num
            '{"person_last_name":"NoIdAtAll"}',                      // nothing to identify it
            '{"person_id":"4004","person_last_name":"Broken"'        // invalid JSON
        }, '\n');
        Test.setMock(HttpCalloutMock.class, new S3Mock(new Map<String, String>{ key => body }));
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, false);
        Long size = Blob.valueOf(body).size();

        sync.execute(null, new List<String>{ '0|' + size + '|' + size + '|' + key });

        System.assertEquals(4, sync.skipped);
        String allErrors = String.join(sync.errors, '\n');
        System.assert(allErrors.contains('skipped - person_id 4001 - person_last_name missing'), allErrors);
        System.assert(allErrors.contains('skipped - person_id blank (person_num N-77) - person_id missing'), allErrors);
        System.assert(allErrors.contains('skipped - person_id blank - person_id missing'), allErrors);
        System.assert(allErrors.contains('line 4: invalid JSON - person_id 4004'), allErrors);
    }

    @IsTest
    static void personIdIsReadFromUnparseableLines() {
        System.assertEquals('person_id 55', SCGOEAWSDataSync.personIdFromRaw('{"person_id": 55, "x":'));
        System.assertEquals('person_id abc-1', SCGOEAWSDataSync.personIdFromRaw('{"person_id":"abc-1"'));
        System.assertEquals('person_id not found in row', SCGOEAWSDataSync.personIdFromRaw('{not json'));
        System.assertEquals('person_id not found in row', SCGOEAWSDataSync.personIdFromRaw(null));
        System.assertEquals('person_id blank', SCGOEAWSDataSync.rowId(null));
    }

    // ---------- Email fallback (AWS Person Id new, email on a Person Account without one) ----------
    private static String rowWithEmail(String id, String lastName, String email) {
        return '{"person_id":"' + id + '","person_last_name":"' + lastName + '","pref_email_address":"' + email + '"}';
    }

    // Inserts Person Accounts the way staff or the migration created them; allowSave so the email duplicate rule
    // (Allow + alert) does not stop the test data itself.
    private static List<Account> personAccounts(List<String> emails, String awsId) {
        Id rt = Schema.SObjectType.Account.getRecordTypeInfosByDeveloperName().get('PersonAccount').getRecordTypeId();
        Database.DMLOptions options = new Database.DMLOptions();
        options.DuplicateRuleHeader.allowSave = true;
        List<Account> accounts = new List<Account>();
        for (Integer i = 0; i < emails.size(); i++) {
            Account a = new Account(RecordTypeId = rt, LastName = 'Staff' + i, PersonEmail = emails[i]);
            if (awsId != null) {
                a.put('AWS_Person_Id__c', awsId);
            }
            a.setOptions(options);
            accounts.add(a);
        }
        Database.insert(accounts, options);
        return accounts;
    }

    // Parses and saves the rows as execute() does, minus the S3 read: these tests insert Accounts first, and a
    // callout is not allowed after DML in the same transaction.
    private static SCGOEAWSDataSync runRows(List<String> rows, Boolean dryRun) {
        String label = DIR + 'email.jsonl';
        SCGOEAWSDataSync sync = new SCGOEAWSDataSync(null, null, dryRun);
        sync.upsertAccounts(sync.parseLines(String.join(rows, '\n'), label), label);
        return sync;
    }

    // One Person Account without AWS Person Id holds the email: it is updated with the id and mapped fields.
    @IsTest
    static void newPersonIdLinksToAccountWithSameEmail() {
        if (!hasExternalId()) {
            return;
        }
        Account staff = personAccounts(new List<String>{ 'link.me@example.com' }, null)[0];

        SCGOEAWSDataSync sync = runRows(new List<String>{ rowWithEmail('5001', 'Aws', 'LINK.me@example.com') }, false);

        System.assertEquals(0, sync.created, String.join(sync.errors, '\n'));
        System.assertEquals(1, sync.updated);
        System.assertEquals(1, sync.linkedByEmail);
        System.assertEquals(0, sync.failed, String.join(sync.errors, '\n'));
        Account after = Database.query('SELECT Id, LastName, AWS_Person_Id__c FROM Account WHERE Id = \'' + staff.Id + '\'');
        System.assertEquals('5001', after.get('AWS_Person_Id__c'));
        System.assertEquals('Aws', after.LastName);
        System.assertEquals(1, Database.countQuery('SELECT COUNT() FROM Account WHERE PersonEmail = \'link.me@example.com\''),
            'no second person created');
    }

    // Next run: the id now exists, so the row matches on the External ID and the email step is not used.
    @IsTest
    static void linkedAccountMatchesOnPersonIdNextRun() {
        if (!hasExternalId()) {
            return;
        }
        personAccounts(new List<String>{ 'again@example.com' }, null);
        runRows(new List<String>{ rowWithEmail('5101', 'First', 'again@example.com') }, false);

        SCGOEAWSDataSync second = runRows(new List<String>{ rowWithEmail('5101', 'Second', 'again@example.com') }, false);

        System.assertEquals(1, second.updated);
        System.assertEquals(0, second.linkedByEmail);
        System.assertEquals(0, second.created);
    }

    // Two Person Accounts without AWS Person Id share the email: ambiguous, not linked, inserted as new.
    @IsTest
    static void ambiguousEmailMatchIsNotLinked() {
        if (!hasExternalId()) {
            return;
        }
        personAccounts(new List<String>{ 'two@example.com', 'two@example.com' }, null);

        SCGOEAWSDataSync sync = runRows(new List<String>{ rowWithEmail('5201', 'Aws', 'two@example.com') }, false);

        System.assertEquals(0, sync.linkedByEmail);
        System.assertEquals(1, sync.warnings.get(SCGOEAWSDataSync.WARN_EMAIL_MANY));
        System.assertEquals(1, sync.created, String.join(sync.errors, '\n'));
        System.assertEquals(2, Database.countQuery(
            'SELECT COUNT() FROM Account WHERE PersonEmail = \'two@example.com\' AND AWS_Person_Id__c = null'));
    }

    // A shared office address used by two rows in one chunk is never linked to either of them.
    @IsTest
    static void emailSharedByRowsInChunkIsNotLinked() {
        if (!hasExternalId()) {
            return;
        }
        Account office = personAccounts(new List<String>{ 'exams@example.edu' }, null)[0];

        SCGOEAWSDataSync sync = runRows(new List<String>{
            rowWithEmail('5301', 'One', 'exams@example.edu'), rowWithEmail('5302', 'Two', 'exams@example.edu')
        }, false);

        System.assertEquals(0, sync.linkedByEmail);
        System.assertEquals(2, sync.warnings.get(SCGOEAWSDataSync.WARN_EMAIL_SHARED));
        System.assertEquals(2, sync.created, String.join(sync.errors, '\n'));
        Account after = Database.query('SELECT AWS_Person_Id__c FROM Account WHERE Id = \'' + office.Id + '\'');
        System.assertEquals(null, after.get('AWS_Person_Id__c'));
    }

    // Today's 189 failures: the AWS Person Id already exists and its email is on another Account.
    // The rule is Allow + alert, so the update must save (allowSave) and the other Account is left alone.
    @IsTest
    static void knownPersonIdSavesEvenWhenEmailIsOnAnotherAccount() {
        if (!hasExternalId()) {
            return;
        }
        Account other = personAccounts(new List<String>{ 'taken@example.com' }, null)[0];
        personAccounts(new List<String>{ null }, '5401');

        SCGOEAWSDataSync sync = runRows(new List<String>{ rowWithEmail('5401', 'Aws', 'taken@example.com') }, false);

        System.assertEquals(0, sync.failed, String.join(sync.errors, '\n'));
        System.assertEquals(1, sync.updated);
        System.assertEquals(0, sync.linkedByEmail);
        Account mine = Database.query('SELECT PersonEmail FROM Account WHERE AWS_Person_Id__c = \'5401\'');
        System.assertEquals('taken@example.com', mine.PersonEmail);
        Account untouched = Database.query('SELECT AWS_Person_Id__c FROM Account WHERE Id = \'' + other.Id + '\'');
        System.assertEquals(null, untouched.get('AWS_Person_Id__c'));
    }

    // Dry run: the email link is rolled back with everything else.
    @IsTest
    static void dryRunRollsBackEmailLink() {
        if (!hasExternalId()) {
            return;
        }
        Account staff = personAccounts(new List<String>{ 'dry@example.com' }, null)[0];

        SCGOEAWSDataSync sync = runRows(new List<String>{ rowWithEmail('5501', 'Aws', 'dry@example.com') }, true);

        System.assertEquals(1, sync.linkedByEmail);
        Account after = Database.query('SELECT AWS_Person_Id__c FROM Account WHERE Id = \'' + staff.Id + '\'');
        System.assertEquals(null, after.get('AWS_Person_Id__c'), 'dry run must roll back');
    }

    // Rows without email, or without any match, keep the normal insert.
    @IsTest
    static void rowsWithoutEmailMatchAreInsertedAsBefore() {
        if (!hasExternalId()) {
            return;
        }
        SCGOEAWSDataSync sync = runRows(new List<String>{
            row('5601', 'NoEmail'), rowWithEmail('5602', 'NoMatch', 'nobody@example.com')
        }, false);

        System.assertEquals(2, sync.created, String.join(sync.errors, '\n'));
        System.assertEquals(0, sync.linkedByEmail);
    }
}