add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE shimanskijvl;

DROP TABLE IF EXISTS kkt;

CREATE external TABLE kkt (  
    kktRegId String,
    content struct<
               userInn:string,
               totalSum:Int, 
               dateTime:struct<
                    datee:BigInt
                    >   
               >,
    subtype String
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true", 'mapping.datee' = '$date')
STORED AS TEXTFILE
LOCATION '/data/hive/fns2';

select * from kkt limit 50;
