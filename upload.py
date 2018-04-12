from hdfs import *

client = Client("http://student62:50070")
client.list("/")
client.makedirs("/Face")
client.list("/")
client.upload("/test","/var/www/html/Spark_SQL/img_store/lala.jpg")
client.list("/test")