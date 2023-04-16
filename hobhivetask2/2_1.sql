add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.exec.reducers.max=8;

USE shimanskijvl;


--select content.userInn, sum(content.totalSum) as sum from kkt_orc where subtype = 'receipt' group by content.userInn order by sum desc limit 50;
select content.userInn, sum(content.totalSum) as sum from kkt where subtype = 'receipt' group by content.userInn order by sum desc limit 50;
--select content.userInn, sum(content.totalSum) as sum from kkt_parfait where subtype = 'receipt' group by content.userInn order by sum desc limit 50;
