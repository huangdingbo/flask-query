# Flask-Query 使用说明

## 一、安装
pip install Flask-Query

## 使用

### 二、Flask配置

#### 1、pymysql方式
```python
app.config["FLASK_QUERY_CONFIG"] = {
    "host": "114.115.165.68",
    "port": "3306",
    "user": "root",
    "password": "FRuGckhYFZ6KpWUV",
    "database": "school",
    "charset": "utf8",
}
```

#### 2、DBUtils连接池
```python
 app.config["FLASK_QUERY_CONFIG"] = PooledDB(
     creator=pymysql,  # 使用链接数据库的模块
     maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
     mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
     maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
     maxshared=3,
     # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的
     # threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
     blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
     maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
     setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
     ping=0,
     # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is
     # created, 4 = when a query is executed, 7 = always
     host='114.115.165.68',
     port=3306,
     user='root',
     password='FRuGckhYFZ6KpWUV',
     database='school',  # 链接的数据库的名字
     charset='utf8'
 )
```

### 三、在项目中使用

#### 1、查询
```python
data = Query().table("Course as c").leftJoin("Teacher as t", on="c.TID=t.TID").select(["t.*,c.*"]).where({"c.id": 1}).all()
```

### 2、插入(insert)
```python

data = {"id": 20, "TSID": "22", "SID": "3", "CID": "2", "score": "2"}

Query().createCommand().insert(table="SC", data=data).execute()
```

### 3、批量插入(bathInsert)
```python

    data = [
        {"TSID": "1", "SID": "1", "CID": "1", "score": "1"},
        {"TSID": "1", "SID": "1", "CID": "1", "score": "1"},
        {"TSID": "1", "SID": "1", "CID": "1", "score": "1"},
        {"TSID": "1", "SID": "1", "CID": "1", "score": "1"},
    ]

    Query().createCommand().bathInsert(table="SC", data=data).execute()
```

### 4、更新(update)
```python

# 传入条件
Query().createCommand().update(table="SC", data=data,where=["and",[">=", "id", "1"],{"id":2}]).execute()

# 使用Query构建where条件
Query().createCommand().update(table="SC", data=data,where=Query().where([">=", "id", "1"]).andWhere({"id": 2}).where_prop).execute()

```

### 5、更新或者插入(upsert)
```python

Query().upsert(table="SC", data=data, ignore=False, primary_key="id").execute()
```

### 6、删除(delete)
```python
Query().createCommand().delete(table='SC', where={"id": 1}).execute()
```

















