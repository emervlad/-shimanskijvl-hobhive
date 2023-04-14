add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;
SET mapreduce.job.reduces=8;

USE shimanskijvl_test;

with tbl as (
    select content.userInn as inn, subtype, LAG(subtype, 1, '') OVER (ORDER BY content.userInn, content.dateTime.datee) as prevtype, LEAD(subtype, 1, '') OVER (ORDER BY content.userInn, content.dateTime.datee) as nexttype from kkt
) select tbl.inn from tbl where tbl.subtype = 'receipt' and tbl.prevtype = 'closeShift' group by tbl.inn order by tbl.inn limit 50;