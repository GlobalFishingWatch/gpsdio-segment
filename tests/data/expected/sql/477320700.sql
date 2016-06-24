SELECT mmsi, lat, lon,  
  FORMAT_UTC_USEC(timestamp) as timestamp, 
  shipname

FROM TABLE_DATE_RANGE([pipeline_classify_logistic_661b.], 
  TIMESTAMP('2015-01-01'), TIMESTAMP('2015-02-01'))

where mmsi in (477320700)
order by timestamp
LIMIT 100
