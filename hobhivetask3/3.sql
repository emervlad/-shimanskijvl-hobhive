add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;
SET hive.exec.reducers.max=8;

USE shimanskijvl;

WITH t1 AS (
    select content.userInn as inn, from_unixtime(cast((content.dateTime.datee / 1000) as int), 'dd') as daay, NVL(sum(content.totalSum), 0 ) as sum from kkt group by content.userInn, 2 order by sum 
)
SELECT r.inn, r.daay, r.sum
FROM   (SELECT e.*, 
               ROW_NUMBER() OVER 
                 (PARTITION BY e.inn ORDER BY e.sum DESC) AS pos
        FROM   t1 AS e) AS r
WHERE  r.pos = 1 and r.inn is not null
ORDER BY r.inn;
