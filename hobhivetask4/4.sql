add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.groupby.orderby.position.alias=true;
SET mapreduce.job.reduces=8;

USE shimanskijvl;
with sum as (
  select content.userInn as inn, from_unixtime(content.dateTime.datee, 'HH') < 13 as daay, round(avg(content.totalSum), 0) as sum from kkt where subtype = 'receipt' group by content.userInn, 2
)
select left_sum.inn, left_sum.sum, right_sum.sum from sum left_sum join sum right_sum on (left_sum.inn = right_sum.inn ) where left_sum.daay and not right_sum.daay and (left_sum.sum > right_sum.sum) order by left_sum.sum limit 50;
