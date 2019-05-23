import pandas as pd
import socket
import messages_pb2 as pbs

# read in data
data = pd.read_csv('CRNS0101-05-2019-AK_Sitka_1_NE.txt', header=None, delimiter='\s+')

data.columns = ['WBANNO', 'UTC_DATE', 'UTC_TIME', 'LST_DATE', 'LST_TIME', \
                'CRX_VN', 'LONGITUDE', 'LATITUDE', 'AIR_TEMPERATURE', 'PRECIPITATION', \
                'SOLAR_RADIATION', 'SR_FLAG', 'SURFACE_TEMPERATURE', 'ST_TYPE', 'ST_FLAG', \
                'RELATIVE_HUMIDITY', 'RH_FLAG', 'SOIL_MOISTURE_5', 'SOIL_TEMPERATURE_5', 'WETNESS', \
                'WET_FLAG', 'WIND_1_5', 'WIND_FLAG']

# create socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# connect to the web server on port 55555
sock.connect(("localhost", 55555))


for i in range(len(data)):

    # create record
    record = pbs.Record()

    # set record attributes
    record.WBANNO = data.loc[i, ['WBANNO']][0]
    record.UTC_DATE = data.loc[i, ['UTC_DATE']][0]
    record.UTC_TIME = data.loc[i, ['UTC_TIME']][0]
    record.LST_DATE = data.loc[i, ['LST_DATE']][0]
    record.LST_TIME = data.loc[i, ['LST_TIME']][0]
    record.CRX_VN = data.loc[i, ['CRX_VN']][0]
    record.LONGITUDE = data.loc[i, ['LONGITUDE']]
    record.LATITUDE = data.loc[i, ['LATITUDE']]
    record.AIR_TEMPERATURE = data.loc[i, ['AIR_TEMPERATURE']]
    record.PRECIPITATION = data.loc[i, ['PRECIPITATION']]
    record.SOLAR_RADIATION = data.loc[i, ['SOLAR_RADIATION']]
    record.SR_FLAG = data.loc[i, ['SR_FLAG']][0]
    record.SURFACE_TEMPERATURE = data.loc[i, ['SURFACE_TEMPERATURE']]
    record.ST_TYPE = data.loc[i, ['ST_TYPE']][0]
    record.ST_FLAG = data.loc[i, ['ST_FLAG']][0]
    record.RELATIVE_HUMIDITY = data.loc[i, ['RELATIVE_HUMIDITY']][0]
    record.RH_FLAG = data.loc[i, ['RH_FLAG']][0]
    record.SOIL_MOISTURE_5 = data.loc[i, ['SOIL_MOISTURE_5']]
    record.SOIL_TEMPERATURE_5 = data.loc[i, ['SOIL_TEMPERATURE_5']]
    record.WETNESS = data.loc[i, ['WETNESS']][0]
    record.WET_FLAG = data.loc[i, ['WET_FLAG']][0]
    record.WIND_1_5 = data.loc[i, ['WIND_1_5']]
    record.WIND_FLAG = data.loc[i, ['WIND_FLAG']][0]


    # send the record
    sock.sendall(record.SerializeToString())