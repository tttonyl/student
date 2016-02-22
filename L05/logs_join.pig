-- Simson L. Garfinkel
-- ANLY502 --- parse the forensicswiki Apache log files using a regular expression.
-- And perform a join

DEFINE EXTRACT       org.apache.pig.piggybank.evaluation.string.EXTRACT();

-- Here is a short sample of the logs:
-- s3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01
-- 
raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01' as (line:chararray);
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012.txt' as (line:chararray);

maxmind  = load 's3://gu-anly502/ps03/maxmind' as (ipaddr:chararray, country:chararray);


logs_base = 
  FOREACH
   raw_logs
  GENERATE
   FLATTEN ( EXTRACT( line,
     '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) \\S+" (\\S+) (\\S+) "([^"]*)" "([^"]*)"'
     ) ) AS (
     host: chararray, identity: chararray, user: chararray, datetime_str: chararray, verb: chararray, url: chararray, request: chararray, status: int,
     size: int, referrer: chararray, agent: chararray
     );

geolocated_logs = JOIN logs_base BY host, maxmind BY ipaddr; 
geolocated_50 = LIMIT geolocated_logs 50;
dump geolocated_50;
