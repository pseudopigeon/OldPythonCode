import pandas as pd
import geocoder
import time, json

USERNAME = 'jd0913'
# infoDf = pd.read_csv('geoData/countryInfo.csv', index_col=0)
# geoname_ids = infoDf['geonameid'].tolist()

testDir = 'geoData/testData/'
admdf = pd.read_csv(testDir + 'admin2.csv', index_col=0)
geoIds = admdf['geonames_id'].tolist()

completed = []
with open(testDir + 'admin3.txt', 'r') as f:
        for line in f:
                id = json.loads(line)
                completed.append(id)

print('Calculating To Do List')
toDo = [x for x in geoIds if x not in completed]

print('Calculating Chunks')
def chunks(geoList, n):
        for i in range(0, len(geoList), n):
                yield geoList[i:i + n]

chunkList = list(chunks(toDo, 1000))
print('Total of {} chunks to download'.format(str(len(chunkList))))
completed_chunks = 0
print('Beginning download')
for chunk in chunkList:
        i = 0
        for geoId in chunk:
                data = geocoder.geonames(geoId, method='children', key=USERNAME)
                if data.ok:
                        geojson = data.geojson
                        jsonList = geojson['features']
                        for item in jsonList:
                                d = item['properties']
                                with open(testDir + 'admin3.txt', 'a') as f:
                                        f.write(json.dumps(d))
                                        f.write('\n')
                        i += 1
                else:
                        with open(testDir + 'badGeoCodes.txt', 'a') as f:
                                f.write(str(geoId))
                                f.write('\n')
                        i += 1
        completed_chunks += 1
        print('Completed {} out of {} chunks, sleeping for hour starting at: {}'.format(str(completed_chunks),str(len(chunkList)), time.ctime()))
        time.sleep(3600)

print('*'* 100)
print('COMPLETED')
print('*'*100)


# def get_geojson(geoId):
        # data = geocoder.geonames(geoId, method='details', key='jd0913')
        # return data.geojson