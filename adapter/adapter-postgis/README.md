# Postgis适配说明
Point类在com.hufudb.openhufu.data.storage.Point中定义

在proto传输过程中，Point类型以字符串类型Point(x y)形式传递

在生成SQL语句时，需要将Point类型翻译为ST_GeomFromText('Point(x y)', 4326)，从而表示为geometry类型对象