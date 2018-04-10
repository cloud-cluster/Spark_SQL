import os
import re

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

#### base set
find_path = (r'/image/', )


class HbaseWrite():
    def __init__(self):
        self.tableName = 'img_store'
        self.transport = TSocket.TSocket('10.42.0.235', 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.transport.open()
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)

    def createTable(self, tableName):
        col1 = ColumnDescriptor(name="data:", maxVersions=1)
        self.client.createTable(tableName, [col1])

    def write(self, PicPath, PicName):
        row = PicName.split('.')[0]
        _data = PicName.split('.')[1]
        PicData = open('%s/%s' % (PicPath, PicName), 'rb').read()
        # 此处需要注意格式，网上的格式报错，少个参数报错如下
        # TypeError: mutateRow() takes exactly 5 arguments (4 given)
        self.client.mutateRow(self.tableName, row, [Mutation(column='data:%s' % _data, value=PicData)], {})

    def read(self, tableName, PicName):
        row = PicName.split('.')[0]
        data_type = PicName.split('.')[1]
        get_data = self.client.get(tableName, row, 'data:%s' % data_type, {})[0]
        if get_data:
            return get_data.value
        else:
            return 'Error'


def main(_path):
    WHB = HbaseWrite()
    WHB.createTable()
    find_file = re.compile(r'^[0-9a-z]*.jpg$')
    # find_walk = os.walk(_path)
    for path, dirs, files in '/var/www/html/spark_sql/img_store':
        for f in files:
            if find_file.search(f):
                path_name = path
                file_name = f
                WHB.write(path_name, file_name)


if __name__ == '__main__':
    for get_path in find_path:
        main(get_path)
