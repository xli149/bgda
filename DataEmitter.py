from EmitterBase import EmitterBase
import json, struct, time, pandas as pd
import glob
import threading


class DataEmitter(EmitterBase):

    def __init__(self, host, port, data_path):
        super(DataEmitter, self).__init__(host, port)
        self.data_path = data_path
        self.data = pd.read_csv(data_path, header=None, delimiter=r'\s+')
        self.data.columns = ['WBANNO', 'UTC_DATE', 'UTC_TIME', 'LST_DATE', 'LST_TIME', 'CRX_VN', 'LONGITUDE',
                             'LATITUDE', 'AIR_TEMPERATURE', 'PRECIPITATION', 'SOLAR_RADIATION', 'SR_FLAG',
                             'SURFACE_TEMPERATURE', 'ST_TYPE', 'ST_FLAG',
                             'RELATIVE_HUMIDITY', 'RH_FLAG', 'SOIL_MOISTURE_5', 'SOIL_TEMPERATURE_5', 'WETNESS',
                             'WET_FLAG', 'WIND_1_5', 'WIND_FLAG']

    def start(self):
        with self.client_socket as sock:
            for index, record in self.data.iterrows():
                # print(index, row)

                s = {'UTC_DATE': record.UTC_DATE,
                     'UTC_TIME': record.UTC_TIME,
                     'LONGITUDE': record.LONGITUDE,
                     'LATITUDE': record.LATITUDE,
                     'AIR_TEMPERATURE': record.AIR_TEMPERATURE,
                     'PRECIPITATION': record.PRECIPITATION,
                     'SOLAR_RADIATION': record.SOLAR_RADIATION,
                     'SURFACE_TEMPERATURE': record.SURFACE_TEMPERATURE,
                     'RELATIVE_HUMIDITY': record.RELATIVE_HUMIDITY,
                     }

                serialized = str.encode(json.dumps(s))

                len_in_binary = struct.pack('!I', len(serialized))

                # send length of next message in four bytes exactly
                sock.sendto(len_in_binary, self.server_addr)

                # send actual message
                sock.sendto(serialized, self.server_addr)

                # time.sleep(0.5)


if __name__ == '__main__':
    txt_file = "2018/CRNS0101-05-2018-KS_Manhattan_6_SSW.txt"
    DataEmitter('localhost', 55555, txt_file).start()

    # txt_files = glob.glob("2006/CRN*.txt")
    #
    # print("num of files: " + str(len(txt_files)))
    #
    # emitters = []
    #
    # for file in txt_files:
    #     print(f"Reading file: {file}")
    #     emitters.append(DataEmitter('localhost', 55554, file))
    #
    # threads = []
    # for emitter in emitters:
    #     t = threading.Thread(target=emitter.start)
    #     threads.append(t)
    #     t.start()
    #     print(f"starting thread {t}")
