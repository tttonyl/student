-- REGISTER file:/home/hadoop/pig/lib/piggybank.jar;
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();
DEFINE REGEX_EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();

-- Log format:
--- 203.82.82.46 - - [03/Jan/2012:00:55:55 -0800] "GET /w/index.php?title=MediaWiki:Print.css&usemsgcache=yes&ctype=text%2Fcss&smaxage=18000&action=raw&maxage=18000 HTTP/1.1" 200 310 "http://www.forensicswiki.org/wiki/File_Carving" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7" 

raw_logs = load 'access_log.gz' as (line:chararray);
logs_base = 
  FOREACH
   raw_logs
  GENERATE
   FLATTEN (
    EXTRACT(
     line,
     '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] "(.+?)" (\\S+) (\\S+) "([^"]*)" "([^"]*)"'
          )
     ) AS (
       host: chararray, identity: chararray, user: chararray, time: chararray, request: chararray, status: int, size: chararray, referrer: chararray, agent: chararray
       )
;

first50 = LIMIT logs_base 50;
dump first50;

