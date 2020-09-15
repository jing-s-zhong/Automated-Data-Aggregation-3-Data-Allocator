!set variable_substitution=true;
use database &{db_name};
create schema if not exists &{sc_name};
--create schema &{sc_name};
use schema &{sc_name};
--
-------------------------------------------------------
-- Create task management tables
-------------------------------------------------------
--
-- DROP SEQUENCE DATA_ALLOCATION_TARGETS_SEQ;
--
CREATE SEQUENCE DATA_ALLOCATION_TARGETS_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_ALLOCATION_TARGETS;
--
CREATE TABLE DATA_ALLOCATION_TARGETS
(
	ID 							NUMBER NOT NULL DEFAULT DATA_ALLOCATION_TARGETS_SEQ.NEXTVAL,
	TARGET_LABEL				TEXT,
	TARGET_TABLE				TEXT NOT NULL,
	BATCH_CONTROL_COLUMN		TEXT,
	BATCH_CONTROL_SIZE			NUMBER,
	BATCH_CONTROL_NEXT			TEXT,
	BATCH_PROCESSED		    	TIMESTAMP_NTZ,
	BATCH_PROCESSING			TIMESTAMP_NTZ,
	BATCH_MICROCHUNK_CURRENT 	TIMESTAMP_NTZ,
	BATCH_SCHEDULE_TYPE			TEXT,
	BATCH_SCHEDULE_LAST			TIMESTAMP_NTZ,
	PATTERN_COLUMNS		    	ARRAY,
	GROUPBY_COLUMNS		    	ARRAY,
	GROUPBY_PATTERN		    	NUMBER,
	GROUPBY_FLEXIBLE			BOOLEAN,
	AGGREGATE_COLUMNS			ARRAY,
	AGGREGATE_FUNCTIONS			ARRAY,
	DEFAULT_PROCEDURE			TEXT,
	CONSTRAINT PK_DATA_ALLOCATION_TARGETS PRIMARY KEY (TARGET_TABLE)
)
CLUSTER BY (TARGET_TABLE)
COMMENT = 'This tableis used to register the allocation target'
;
--
-- DROP SEQUENCE DATA_ALLOCATION_BUCKETS_SEQ;
--
CREATE SEQUENCE DATA_ALLOCATION_BUCKETS_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_ALLOCATION_BUCKETS;
--
CREATE TABLE DATA_ALLOCATION_BUCKETS
(
	ID 							NUMBER NOT NULL DEFAULT DATA_ALLOCATION_BUCKETS_SEQ.NEXTVAL,
	BUCKET_LABEL				TEXT,
	TARGET_TABLE	        	TEXT NOT NULL,
	BUCKET_TABLE	        	TEXT NOT NULL,
	BUCKET_ENABLED	        	BOOLEAN,
	PATTERN_DEFAULT	        	NUMBER,
	PATTERN_FLEXIBLE	    	BOOLEAN,
	DATA_AVAILABLETIME	    	TIMESTAMP_NTZ,
	DATA_CHECKSCHEDULE	    	TIMESTAMP_NTZ,
	TRANSFORMATION	        	TEXT,
	CONSTRAINT PK_DATA_ALLOCATION_BUCKETS PRIMARY KEY (TARGET_TABLE, BUCKET_TABLE),
	CONSTRAINT FK_DATA_ALLOCATION_BUCKETS_TARGET_TABLE FOREIGN KEY (TARGET_TABLE)
		REFERENCES DATA_ALLOCATION_TARGETS(TARGET_TABLE)
)
CLUSTER BY (TARGET_TABLE, BUCKET_TABLE)
COMMENT = 'This tableis used to register the allocation buckets'
;
--
-- DROP SEQUENCE DATA_AGGREGATION_LOGGING_SEQ;
--
--CREATE SEQUENCE DATA_AGGREGATION_LOGGING_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_LOGGING;
--
--CREATE TABLE DATA_AGGREGATION_LOGGING
--(
--	EVENT_ID 					NUMBER NOT NULL DEFAULT DATA_AGGREGATION_LOGGING_SEQ.NEXTVAL,
--	EVENT_TIME	    	        TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
--	EVENT_TARGET	        	TEXT,
--	EVENT_SOURCE	        	TEXT,
--	EVENT_STATE					TEXT,
--	EVENT_QUERY					TEXT
--)
--COMMENT = 'This tableis used to log the error of running the processing'
--;
--!set variable_substitution=false;
-------------------------------------------------------
-- Create aggregator stored procedures
-------------------------------------------------------
--
-- Aggregate generation stored procedues for indivual source
-- DROP PROCEDURE DATA_ALLOCATOR(VARCHAR, BOOLEAN, BOOLEAN, BOOLEAN, VARCHAR);
CREATE OR REPLACE PROCEDURE  DATA_ALLOCATOR (
	TARGET_TABLE VARCHAR,
	SCRIPT_ONLY BOOLEAN,
	LOG_DETAILS BOOLEAN,
	NON_ENABLED BOOLEAN,
	BATCH_TIMETAG VARCHAR
	)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT STRICT
AS
$$
var sqlScript = '', pageBreaker = '';

var sourceQuery = `SELECT
	  d.TARGET_TABLE,
	  d.BATCH_CONTROL_COLUMN,
	  d.BATCH_CONTROL_SIZE,
	  d.BATCH_CONTROL_NEXT,
	  d.PATTERN_COLUMNS,
	  d.GROUPBY_COLUMNS,
	  CASE WHEN GROUPBY_FLEXIBLE THEN BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT) ELSE d.GROUPBY_PATTERN END GROUPBY_PATTERN,
	  d.GROUPBY_FLEXIBLE OR (d.GROUPBY_PATTERN = BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT)) GROUPBY_COMPITABLE,
	  d.GROUPBY_FLEXIBLE AND s.PATTERN_FLEXIBLE PATTERN_FLEXIBLE,
	  d.AGGREGATE_COLUMNS,
	  d.AGGREGATE_FUNCTIONS,
	  d.DEFAULT_PROCEDURE,
	  s.BUCKET_LABEL,
	  s.BUCKET_TABLE,
	  s.TRANSFORMATION
  FROM DATA_ALLOCATION_TARGETS d
  JOIN DATA_ALLOCATION_BUCKETS s
  USING(TARGET_TABLE)
  WHERE d.TARGET_TABLE = :1
	AND s.BUCKET_ENABLED != :2;`;

var sourceStmt = snowflake.createStatement({
	sqlText: sourceQuery,
	binds: [TARGET_TABLE, NON_ENABLED]
});

var sources = sourceStmt.execute();

// loop each source
while (sources.next()) {
	var targetTable = sources.getColumnValue(1);
	var batchControlColumn = sources.getColumnValue(2);
	var batchControlSize = sources.getColumnValue(3);
	var batchControlNext = sources.getColumnValue(4);
	var patternColumns = sources.getColumnValue(5);
	var groupByColumns = sources.getColumnValue(6).map(x => x.split(':')[1]);
	var dimensionColumns = sources.getColumnValue(6).map(x => x.split(':')[0]);
	var groupByPattern = sources.getColumnValue(7);
	var groupByCompitable = sources.getColumnValue(8);
	var patternFlexible = sources.getColumnValue(9);
	var aggregateColumns = sources.getColumnValue(10).map(x => x.split(':')[1]);
	var measureColumns = sources.getColumnValue(10).map(x => x.split(':')[0]);
	var aggregateFunctions = sources.getColumnValue(11);
	var defaultProcedure = sources.getColumnValue(12);
	var bucketLabel = sources.getColumnValue(13);
	var bucketTable = sources.getColumnValue(14);
	var transformation = sources.getColumnValue(15);
	var sourceTitle = '', sqlExecuted = '', sqlResult = '(SP call parameter script_only is presented true)';

	if (transformation) { transformation = '(' + transformation + ')' } else { transformation = targetTable }

	if (groupByCompitable) {
		var flagIndexLast = patternColumns.length - 1,
			patternSegment = groupByPattern;
		var selectList = groupByColumns[0] === "DATA_PATTERN" ? (patternFlexible ? 'BITOR(' + groupByColumns[0] + ',' + groupByPattern + ')' : groupByPattern) + ' ' : '',
			dimensionList = '',
			groupByList = '',
			columnSplitter = '';
		for (var i = 0; i <= flagIndexLast; i++) {
			var flagPower = 2 ** (flagIndexLast - i);
			if (patternSegment / flagPower < 1) {
				dimensionList = dimensionList + columnSplitter + dimensionColumns[groupByColumns.indexOf(patternColumns[i])];
				selectList = selectList + columnSplitter + patternColumns[i];
				groupByList = groupByList + columnSplitter + patternColumns[i];
				columnSplitter = ',';
			}
			patternSegment %= flagPower;
		}

		var targetAlias = 'T.', sourceAlias = 'S.';
		var loadQuery = `MERGE INTO ` + bucketTable + ` ` + targetAlias[0] + ` \n`
			+ `USING ( \n`
            + `  SELECT ` + dimensionList + ',' + measureColumns.map(x => {return 'IFNULL(IFNULL(A.' + x + ',0)/NULLIF(D.' + x +',0), 1) ' + x + '_RATIO'}) + ` \n`
            + `  FROM ( \n`
			+ `    SELECT ` + dimensionList + `,` + aggregateFunctions.map((x, i) => { return x.replace('?', measureColumns[i]) + ' ' + measureColumns[i] }) + ` \n`
			+ `    FROM ( \n`
			+ `      SELECT ` + dimensionList + `,` + measureColumns + ` \n`
			+ `      FROM ` + transformation + ` \n`
			+ `      WHERE ` + batchControlColumn + ` >= :1 AND ` + batchControlColumn + ` < ` + batchControlNext + ` \n`
			+ `      ) \n`
			+ `    GROUP BY ` + dimensionList + ` \n`
            + `    ) A \n`
            + `  FULL JOIN ( \n`
			+ `    SELECT ` + dimensionList + `,` + aggregateFunctions.map((x, i) => { return x.replace('?', measureColumns[i]) + ' ' + measureColumns[i] }) + ` \n`
			+ `    FROM ( \n`
			+ `      SELECT ` + dimensionList + `,` + measureColumns + ` \n`
			+ `      FROM ` + bucketTable + ` \n`
			+ `      WHERE ` + batchControlColumn + ` >= :1 AND ` + batchControlColumn + ` < ` + batchControlNext + ` \n`
			+ `      ) \n`
			+ `    GROUP BY ` + dimensionList + ` \n`
            + `    ) D \n`
			+ `  USING (` + dimensionList + `) \n`
			+ `  ) ` + sourceAlias[0] + ` \n`
			+ `ON ` + dimensionList.split(',').map((x, i) => { return `COALESCE(TO_CHAR(` + targetAlias + x + `),'') = COALESCE(TO_CHAR(` + sourceAlias + x + `),'')` }).join('\n AND ') + ` \n`
			+ `WHEN MATCHED THEN UPDATE SET ` + measureColumns.map(x => { return x + '_RATIO = ' + sourceAlias[0] + `.` + x + '_RATIO'}) + ` \n`
//			+ `WHEN NOT MATCHED THEN INSERT(` + dimensionList + `,` + measureColumns + `) \n`
//			+ `VALUES (` + groupByList.split(',').map(x => { return sourceAlias[0] + `.` + x }) + `,` + aggregateColumns.map(x => { return sourceAlias[0] + `.` + x }) + `) \n`
			+ `;`;

		sqlExecuted = loadQuery.replace(/:2/g, batchControlSize).replace(/:1/g, "'" + BATCH_TIMETAG + "'");

		if (!SCRIPT_ONLY) {
			try {
				var loadStmt = snowflake.createStatement({
					sqlText: loadQuery,
					binds: [BATCH_TIMETAG, batchControlSize]
				});
				loadStmt.execute();
				sqlResult = '[INFO-1] Successfully loaded data into target table'
			}
			catch (err) {
				sqlResult = '[ERROR] Failure to load data into target table => ' + err
			}
			finally {
				if (LOG_DETAILS || sqlResult.startsWith('[ERROR]')) {
					var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATE, EVENT_QUERY) VALUES(:1, :2, :3, :4)';
					var logStmt = snowflake.createStatement({
						sqlText: logQuery,
						binds: [targetTable, bucketTable, sqlResult, sqlExecuted]
					});
					logStmt.execute()
				}
			}
		}
	}
	else {
		sqlExecuted = '-- No data is loaded from this source as the data pattern is incompatible!';
	}

	sourceTitle = pageBreaker + '-'.repeat(65)
		+ `\n-- BUCKET_LABEL: ` + bucketLabel
		+ `\n-- BUCKET_TABLE: ` + bucketTable.replace('DATAMART.BUYSIDE_NETWORK.', '').replace('DATAMART.SELLSIDE_NETWORK.', '')
		+ `\n-- SOURCE STATE: ` + sqlResult
		+ `\n` + '-'.repeat(65) + `\n`;
	sqlScript = sqlScript + sourceTitle + sqlExecuted;
	pageBreaker = `\n\n`;
}

return sqlScript;
$$;
--
-- Aggregate stored procedues to loop all available source tables
-- DROP PROCEDURE DATA_ALLOCATOR(VARCHAR, BOOLEAN, BOOLEAN);
--
CREATE OR REPLACE PROCEDURE DATA_ALLOCATOR (
	TARGET_TABLE VARCHAR,
	SCRIPT_ONLY BOOLEAN,
	LOG_DETAILS BOOLEAN
	)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT STRICT
AS
$$
var NON_ENABLED = 0;

var batchControlColumn = '',
	batchControlSize = 0,
	batchControlType = '',
	batchLoopTag = '',
	batchLoopEnd = '',
	batchScheduleCurrent;
var loopScript = '',
	pageBreaker = '',
	loopSegmenter = '',
	callResult = '(SP call parameter script_only is presented true)';

//
// Detect runable or not
//
var targetQuery = `SELECT BATCH_CONTROL_COLUMN,
  BATCH_CONTROL_SIZE,
  BATCH_SCHEDULE_TYPE,
  --DATEADD(MINUTE, BATCH_CONTROL_SIZE, BATCH_PROCESSED) BATCH_LOOP_BEGIN,
  --DATEADD(MINUTE, -BATCH_CONTROL_SIZE, BATCH_POSSIBLE) BATCH_LOOP_END,
  DATEADD(MINUTE, CASE BATCH_SCHEDULE_TYPE
	  WHEN 'MINUTES' THEN BATCH_CONTROL_SIZE
	  WHEN 'HOURLY' THEN 60
	  ELSE 1440
	END, BATCH_PROCESSED) BATCH_LOOP_BEGIN,
  BATCH_POSSIBLE BATCH_LOOP_END,
  BATCH_SCHEDULE_CURRENT
FROM (
  SELECT BATCH_CONTROL_COLUMN,
	  BATCH_CONTROL_SIZE,
	  BATCH_CONTROL_NEXT,
	  BATCH_PROCESSED,
	  BATCH_PROCESSING,
	  BATCH_SCHEDULE_TYPE,
	  BATCH_SCHEDULE_LAST,
	  CURRENT_TIMESTAMP() BATCH_SCHEDULE_CURRENT,
	  CASE BATCH_SCHEDULE_TYPE
		WHEN 'HOURLY' THEN DATE_TRUNC(HOUR, BATCH_SCHEDULE_CURRENT)
		WHEN 'DAILY' THEN DATE_TRUNC(DAY, BATCH_SCHEDULE_CURRENT)
		ELSE DATEADD(MINUTE,FLOOR(DATEDIFF(MINUTE,'1970-01-01',BATCH_SCHEDULE_CURRENT)/BATCH_CONTROL_SIZE)*BATCH_CONTROL_SIZE,'1970-01-01')
	  END BATCH_POSSIBLE
  FROM (
	SELECT BATCH_CONTROL_COLUMN,
		BATCH_CONTROL_SIZE,
		BATCH_CONTROL_NEXT,
		BATCH_PROCESSED,
		BATCH_PROCESSING,
		BATCH_SCHEDULE_TYPE,
		BATCH_SCHEDULE_LAST,
		CURRENT_TIMESTAMP() BATCH_SCHEDULE_CURRENT
	FROM DATA_ALLOCATION_TARGETS
	WHERE TARGET_TABLE = :1
	)
  )
WHERE BATCH_PROCESSING IS NULL
OR DATEDIFF(MINUTE, BATCH_SCHEDULE_LAST, BATCH_POSSIBLE) > BATCH_CONTROL_SIZE;`;

var targetStmt = snowflake.createStatement({
	sqlText: targetQuery,
	binds: [TARGET_TABLE]
});

var target = targetStmt.execute();

if (target.next()) {
	batchControlColumn = target.getColumnValue(1);
	batchControlSize = target.getColumnValue(2);
	batchScheduleType = target.getColumnValue(3);
	batchLoopTag = target.getColumnValue(4);
	batchLoopEnd = target.getColumnValue(5);
	batchScheduleCurrent = target.getColumnValue(6);
}
else {
	return '\n\n-- Skip this schedule as previous schedule has not done yet!\n'
}

//
// Initialize the batch exclusion control context
//
var contextQuery = `UPDATE DATA_ALLOCATION_TARGETS \n `
	+ `SET BATCH_PROCESSING = :2, \n\t `
	+ `BATCH_SCHEDULE_LAST = :3 \n`
	+ `WHERE TARGET_TABLE = :1;`;
var contextStmt = snowflake.createStatement({
	sqlText: contextQuery,
	binds: [TARGET_TABLE, batchLoopEnd, batchScheduleCurrent]
});

if (!SCRIPT_ONLY) { contextStmt.execute(); }

//
// Loop and call the date_poplate SP for each batch
//
while (batchLoopTag <= batchLoopEnd) {
	var contextQuery = `UPDATE DATA_ALLOCATION_TARGETS \n `
		+ `SET BATCH_MICROCHUNK_CURRENT = :2 \n `
		+ `WHERE TARGET_TABLE = :1;`;
	var contextStmt = snowflake.createStatement({
		sqlText: contextQuery,
		binds: [TARGET_TABLE, batchLoopTag.toISOString()]
	});
	if (!SCRIPT_ONLY) { contextStmt.execute(); }

//	var deleteQuery = `DELETE FROM ` + TARGET_TABLE
//		+ ` WHERE ` + batchControlColumn + ` >= :1`
//		+ ` AND ` + batchControlColumn + ` < DATEADD(MINUTE, :2, :1);\n`;
//	var deleteScheduled = deleteQuery
//		.replace(/:2/g, batchControlSize.toString())
//		.replace(/:1/g, '\'' + batchLoopTag.toISOString() + '\'');

	var callQuery = `CALL DATA_ALLOCATOR (:1, :2, :3, :4, :5);\n`;
	var callScheduled = callQuery
		.replace(/:1/g, '\'' + TARGET_TABLE + '\'')
		.replace(/:2/g, SCRIPT_ONLY.toString())
		.replace(/:3/g, LOG_DETAILS.toString())
		.replace(/:4/g, NON_ENABLED.toString())
		.replace(/:5/g, '\'' + batchLoopTag.toISOString() + '\'');

	if (!SCRIPT_ONLY) {
//		try {
//			var removalStmt = snowflake.createStatement({
//				sqlText: deleteQuery,
//				binds: [batchLoopTag.toISOString(), batchControlSize]
//			});
//			removalStmt.execute();
//			callResult = '[INFO-1] Successfully deleted the existing data for reloading';
//		}
//		catch (err) {
//			callResult = '[ERROR] Failure to delete the existing data from target table => ' + err
//		}
//		finally {
//			if (LOG_DETAILS || callResult.startsWith('[ERROR]')) {
//				var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATE, EVENT_QUERY) VALUES(:1, :2, :3, :4)';
//				var logStmt = snowflake.createStatement({
//					sqlText: logQuery,
//					binds: [TARGET_TABLE, '(*** All loaded data covered by current batch ***)', callResult, deleteScheduled]
//				});
//				logStmt.execute()
//			}
//		}

		if (LOG_DETAILS) {
			var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATE, EVENT_QUERY) VALUES(:1, :2, :3, :4)';
			var logStmt = snowflake.createStatement({
				sqlText: logQuery,
				binds: [TARGET_TABLE, '(*** All enabled data sources ***)', '[INFO-2] Make a batch data load call', callScheduled]
			});
			logStmt.execute()
		}

		try {
			var callStmt = snowflake.createStatement({
				sqlText: callQuery,
				binds: [TARGET_TABLE, SCRIPT_ONLY.toString(), LOG_DETAILS.toString(), NON_ENABLED.toString(), batchLoopTag.toISOString()]
			});
			callStmt.execute();
			callResult = '[INFO-2] Successfully completed the batch load call';
		}
		catch (err) {
			callResult = '[ERROR] Failure to complete the batch load call => ' + err
		}
		finally {
			if (LOG_DETAILS || callResult.startsWith('[ERROR]')) {
				var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATE, EVENT_QUERY) VALUES(:1, :2, :3, :4)';
				var logStmt = snowflake.createStatement({
					sqlText: logQuery,
					binds: [TARGET_TABLE, '', callResult, callScheduled]
				});
				logStmt.execute()
			}
		}
	}

	loopSegmenter = pageBreaker + '-'.repeat(65)
		+ `\n-- LOOP FRAME: ` + batchControlColumn + ` = ` + batchLoopTag.toISOString()
		+ `\n-- LOOP CHUNK: ` + batchControlSize.toString() + ` minutes by ` + batchControlColumn
		+ `\n` + '-'.repeat(65) + `\n`;
//	loopScript = loopScript + loopSegmenter + deleteScheduled + callScheduled;
	loopScript = loopScript + loopSegmenter + callScheduled;
	pageBreaker = `\n\n`;

	batchLoopTag.setMinutes(batchLoopTag.getMinutes() + batchControlSize);
}

//
// Clear the batch exclusion control context
//
var contextQuery = `UPDATE DATA_ALLOCATION_TARGETS T \n`
	+ `SET BATCH_MICROCHUNK_CURRENT = NULL, BATCH_PROCESSING = NULL, BATCH_PROCESSED = S.DATA_AVAILABLETIME \n`
	+ `FROM ( \n`
	+ `SELECT d.TARGET_TABLE, MIN(COALESCE(s.DATA_AVAILABLETIME, d.BATCH_PROCESSED)) DATA_AVAILABLETIME \n`
	+ `FROM DATA_ALLOCATION_TARGETS d \n`
	+ `JOIN DATA_ALLOCATION_BUCKETS s \n`
	+ `USING(TARGET_TABLE) \n`
	+ `WHERE s.BUCKET_ENABLED = True \n`
	+ `GROUP BY d.TARGET_TABLE \n`
	+ `) S \n`
	+ `WHERE T.TARGET_TABLE = S.TARGET_TABLE AND T.TARGET_TABLE = :1;`;
var contextStmt = snowflake.createStatement({
	sqlText: contextQuery,
	binds: [TARGET_TABLE]
});

if (!SCRIPT_ONLY) { contextStmt.execute(); }

return loopScript;
$$;
