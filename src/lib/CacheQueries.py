import json
import datetime
import os
import sys

#queryTable = 'OptionsQuery'
queryTable = 'OPTIONS_QUERY'

sys.path.append('../lib/')
from code_library import snowconnection

session          = snowconnection()    
options_query    = session.table('"%s"' % (queryTable))

path = os.path.realpath(__file__) 
dir = os.path.dirname(path)
dir = dir.replace('\src\lib', '\src\csv') 
os.chdir(dir) 

stageR           = open('../csv/toStageQueryResult.csv', 'w')

stageR.write('SK|RESULT_CACHE|RESULT_CACHE_TS\n')

query   = options_query.select(['SK', 'QUERY']).to_pandas().values.tolist() # Rerun all queries
total = len(query)
count = 0
print(str(total) + ' queries to cache')
for row in query:

    result = session.sql(row[1]).collect()

    if(len(result) == 0 or result[0] == None):
        result_str = 'No results'
    elif(len(result[0]) == 0 or result[0][0] == None):
        result_str = 'No results'
    else:
        result_str = result[0][0].replace('\'', '\'\'')

    stageR.write('%d|%s|%s\n' % (row[0], result_str, str(datetime.datetime.now())))

    count += 1
    if(count % 50 == 0):
        print( '%d (%d%%)' % (count, (100.0*count)/total) )


stageR.close()
print('%d queries run' % (count))

path = os.path.realpath(__file__) 
dir = os.path.dirname(path) 
dir = dir.replace('\src\lib', '') 
os.chdir(dir)

print('Clearing Stage...')
session.sql('REMOVE @query_result_stage;').collect()
print('Uploading to Stage...')
session.sql('PUT file://%s/src/csv/toStageQueryResult.csv @query_result_stage;' % (str(os.getcwd()))).collect()
print('Creating Temp Table...')
session.sql('CREATE TEMPORARY TABLE TempQueryResults (SK INT, RESULT_CACHE VARCHAR(1000), RESULT_CACHE_TS TIMESTAMP);').collect()
print('Copying from Stage...')
session.sql('COPY INTO TempQueryResults (SK, RESULT_CACHE, RESULT_CACHE_TS) FROM @query_result_stage file_format = (type = \'CSV\' SKIP_HEADER = 1 FIELD_DELIMITER = \'|\');').collect()
print('Updating Query Table')
session.sql('UPDATE PC."%s" SET RESUlT_CACHE = B.RESUlT_CACHE , RESULT_CACHE_TS = B.RESULT_CACHE_TS FROM TempQueryResults B WHERE PC."%s".SK = B.SK;' % (queryTable, queryTable)).collect()
