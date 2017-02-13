# -*- coding: utf-8 -*-

import sys
import datetime
import redis
import cx_Oracle
import configparser
import os

def loadTask(tslsh, kpyf, fplx_dm, sjlx_dm, mode_, fpsl):
    print u"========================== 任务信息 ============================"
    print u"推送流水:" + tslsh
    print u"开票月份:" + kpyf
    print u"发票类型:" + fplx_dm
    print u"数据类型:" + sjlx_dm
    print u"记录数量:" + str(fpsl)
    print u"目标代理:" + proxy[kpyf[-2:]]

    # 获取本任务的配置信息
    conf = mconf[fplx_dm + sjlx_dm]
    sql = conf["sql"] + " where tslsh = '" + tslsh + "' and kpyf = '" + kpyf + "' "
    col = conf["col"]
    url = proxy[kpyf[-2:]]

    # 查出数据，调用redis 加载
    c.execute(sql)
    rows = c.fetchmany(batch)
    while len(rows) != 0:
        if sjlx_dm == "01":
            load_01(rows, url, col)
        elif sjlx_dm == "02":
            load_02(rows, url, col)

        rows = c.fetchmany(batch)


# 加载发票类信息
def load_01(rows, url, col):
    u = str.split(str(url), ":", -1)
    c = str.split(col, ",", -1)
    r = redis.StrictRedis(host=u[0], port=int(u[1]), db=0)

    # pipe = r.pipeline()  # python 下pipe 对代理不好使

    # 对每一行数据进行处理，分出key, field, expire
    for row in list(rows):
        key = row[0]  # key 需要单独提取出来，剩下的组织成map
        expire = row[len(row)-1] # 最后一项为 expire
        m = {}
        for i in range(1, len(row)-2):
            m[c[i]] = row[i]

        r.hmset(key, m)
        r.expire(key, expire)
        # pipe.hmset(key, m)

    # pipe.execute()

    print str(datetime.datetime.now()) + u" 本次加载数量 " + str(len(rows))


# 加载货物类信息
def load_02(rows, url, col):
    u = str.split(str(url), ":", -1)
    c = str.split(col, ",", -1)
    r = redis.StrictRedis(host=u[0], port=int(u[1]), db=0)

    # pipe = r.pipeline()  # python 下pipe 对代理不好使

    # 对每一行数据进行处理，分出key, field, expire
    for row in list(rows):
        key = row[0]  # key 需要单独提取出来，剩下的组织成map
        m = {}
        m[row[1]] = row[2]
        # print m

        r.hmset(key, m)
        # pipe.hmset(key, m)

    # pipe.execute()

    print str(datetime.datetime.now()) + u" 本次加载数量 " + str(len(rows))


# 加载作废发票信息
def load_03(rows, url, col):
    u = str.split(str(url), ":", -1)
    c = str.split(col, ",", -1)
    r = redis.StrictRedis(host=u[0], port=int(u[1]), db=0)

    # pipe = r.pipeline()  # python 下pipe 对代理不好使

    # 对每一行数据进行处理，分出key, field, expire
    for row in list(rows):
        key = row[0]  # key 需要单独提取出来，剩下的组织成map
        m = {}
        m[row[1]] = row[2]
        # print m
        if r.hexists(key,row[1]):  # 如果有这个field 才修改，防止新增一个redis 记录(有作废票没redis 记录)
            r.hmset(key, m)
        # pipe.hmset(key, m)

    # pipe.execute()

    print str(datetime.datetime.now()) + u" 本次加载数量 " + str(len(rows))


# 根据加载的情况，封闭任务
def finishTask(tslsh, kpyf, fplx_dm, sjlx_dm, mode_):
    # update 重新连接数据库 覆盖全局数据库 conn 。否则 154 行的fetchmany 会报错
    try:
        # 连接数据库
        conn = cx_Oracle.connect(uid)
        # 获取cursor
        c = conn.cursor()
    except:
        print u"连接数据库失败！"
        t, v, _ = sys.exc_info()
        print(t, v)
        sys.exit(1)

    # 根据加载的情况更新数据库
    sql_upd = "update fpcy_sjjzjk set czzt_dm = '2' " + \
              "where 1 = 1 and tslsh = :1 and fplx_dm = :2 and sjlx_dm = :3 and kpyf = :4 and mode_ = :5"

    c.execute(sql_upd, [tslsh, fplx_dm, sjlx_dm, kpyf, mode_])
    conn.commit()

    c.close()
    conn.close()


#####################################################################################


#####################################################################################

## main

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

try:
    # 不要使用 configparser.ConfigParser() 。否则sql 里包含 % 会抛出解析错误
    # config = configparser.ConfigParser()
    config = configparser.RawConfigParser()
    config.read("load.conf")

    uid = config.get("default", "uid")
    batch = config.getint("default", "batch")

    # 查询加载字段的sql
    sql_conf = config.get("config", "sql_conf")
    # 获取待加载任务的sql
    sql_task = config.get("config", "sql_task")

    # 每个月对应的代理地址  {"01":"172.30.11.230:6379", ...}
    proxy = {}
    for key in config.options("proxy"):
        proxy[key] = config.get("proxy", key)

except:
    print u"解析配置文件失败！"
    t, v, _ = sys.exc_info()
    print(t, v)
    sys.exit(1)

try:
    # 连接数据库
    conn = cx_Oracle.connect(uid)
    # 获取cursor
    c = conn.cursor()
except:
    print u"连接数据库失败！"
    t, v, _ = sys.exc_info()
    print(t, v)
    sys.exit(1)

# 从数据库中获取 redis 加载时需要配置信息。
# 对于每个任务，根据任务的 （发票类型，数据类型） 的值取出对应配置
# mconf 存放 取数语句和 hash 列表
mconf = {}
c.execute(sql_conf)
confs = c.fetchall()
for (fplx_dm, sjlx_dm, sql_text, cols) in confs:
    mconf[fplx_dm + sjlx_dm] = {"sql": sql_text, "col": cols}

# 查询要加载的任务
c.execute(sql_task)
tasks = c.fetchmany(batch)
while len(tasks) != 0:
    for (tslsh, kpyf, fplx_dm, sjlx_dm, mode_, fpsl) in tasks:

        try:
            loadTask(tslsh, kpyf, fplx_dm, sjlx_dm, mode_, fpsl)
            # 如果上面不报错，就封闭此任务
            finishTask(tslsh, kpyf, fplx_dm, sjlx_dm, mode_)
        except:
            t, v, _ = sys.exc_info()
            print(t, v, _)
            # 报错了，就什么也不做，并停止后面的任务(这里直接退出程序)，退出前关闭游标和连接
            c.close()
            conn.close()
            sys.exit(1)

    tasks = c.fetchmany(batch)

c.close()
conn.close()
