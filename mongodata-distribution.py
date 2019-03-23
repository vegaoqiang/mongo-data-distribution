import os
import sys
import json
import datetime
import platform
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout

if platform.system() == "Darwin":
    MongoHost = "your mongo host" # mongos1
else:
    MongoHost = "your mongo host" # mongos1 inner ip address

uri = 'mongodb://%s:%s@%s' % (quote_plus("mongo admin user"), quote_plus("mongo passwd"), MongoHost)

client = MongoClient(uri)
db = client["your databases name"]
num = 0 # 第几张图, 由画图函数修改
FileBytesAvailableForReuse = {} # 数据由函数load_collection_stats生成, 节省读数据库次数

AllWeNeedFields = (
        # 你需要展示数据库的信息, 如:
        # "objects",
        # "dataSize",
        "storageSize",
        # "indexes",
        "indexSize"
)

AllWeNeedCollections = (
    # 你需要展示的collections, 如:
    "ASORank",
    "ASORankUS",
    "SearchRankHistory",
    "SearchRankHistoryUS",
)
total_num = len(AllWeNeedCollections) + len(AllWeNeedFields)
fig, ax = plt.subplots(nrows=total_num + 1 , ncols=1, figsize=(15, 20)) # +1 for FileBytesAvailableForReuse

#print(client["appdata"].collection_names())

def load_all_shard():
    """get all shard from mongo cluster"""
    MongoShards = {}
    shard_dict = client["admin"].command("listShards")
    for shard in shard_dict["shards"]:
        # MongoShards[shard["_id"]] = shard["host"].split(":")[0]
        MongoShards[shard["host"].split(":")[0]] = shard["_id"]
    return MongoShards

def collection_not_exist():
    """
    if a new collection created in mongodb, but not in yesterday collection saved file,
    return a data like follow:
    {
        "shard0000": 0,
        "shardxxxx": 0,
        ...
    }
    """
    context = {"total": 0}
    for shard in AllShardsDict.values():
        context[shard] = 0
    return context
    

def recyclable_space(size, shard):
    """
    Read data from db.collection.stats().shard.wiredTiger.block-manager.
    a shard value is count from all collection in AllWeNeedCollections.
    return data and format like follow:
    {
        "shard0000": xxxx
        ...
    }
    """
    global FileBytesAvailableForReuse
    if FileBytesAvailableForReuse.get(shard):
        # 之所以使用两个round是防止size很小的情况, 转换成GB后会变成一个很小的小数, 
        # 原始值和其相加后也会变成一个带长位的小数, 如:152.1 + 0.0000000000002
        FileBytesAvailableForReuse[shard] = round(FileBytesAvailableForReuse[shard] + round(size / 1024**3, 1), 1)
    else:
        FileBytesAvailableForReuse[shard] = round(size / 1024**3, 1)


def load_collection_stats():
    """
    Read data from db.collection.stats(), return data format as follow:
    {
        "collection": {
            "shardxxx": xxxx,
            ....
        }
        ....
    }
    more datails see db command: db.collection.stats()
    """
    ResultDict = defaultdict(dict)
    for collection in AllWeNeedCollections:
        try:
            CollStats = db.command("collstats", collection)
        except ExecutionTimeout:
            sys.exit("load data from mongo timeout")
        ResultDict[collection]["total"] = round(CollStats.get("storageSize") / 1024**3, 1)
        for shard in AllShardsDict.values():
            ShardData = CollStats["shards"].get(shard)
            if ShardData:
                ResultDict[collection][shard] = round(ShardData["storageSize"] / 1024**3, 1)
                recyclable_space(
                    ShardData["wiredTiger"]["block-manager"]["file bytes available for reuse"],
                    shard
                )
            else:
                ResultDict[collection][shard] = 0
    save_today_stats(ResultDict, "collection")
    save_today_stats(FileBytesAvailableForReuse, "available.reuse")
    return ResultDict


def load_dbstats():
    """
    Organizational data format, read data from db.stats(), return data format like
    {
        "dataSize": {
            "shard0000": xxx,
            "shardxxxx": xxx
            ...
        }
        ...    
    }
    """
    ResultDict = defaultdict(dict)
    all_shards = AllShardsDict
    try:
        DBStats = db.command("dbstats")
    except ExecutionTimeout:
        sys.exit("load data from mongo timeout")
    StatsOnShards =  DBStats.pop("raw") # delete raw, just Reserved total data, more details see db.stats()
    for field in AllWeNeedFields:
        ResultDict[field]["total"] = trans_size_unit(DBStats, field)
        for ip, stats in StatsOnShards.items():
            ip = ip.split(":")[0]
            ResultDict[field][all_shards[ip]] = trans_size_unit(stats, field)
    save_today_stats(ResultDict, "stats")
    return ResultDict

def trans_size_unit(stats, field):
    """
    Converse data size unit, from byte to GB
    """
    if field == "indexes" or field == "objects":
        return int(stats.get(field))
    return round(stats.get(field)/1024**3, 1)

def get_unit(field):
    if field == "indexes" or field == "objects":
        return "Int"
    return "GB"


def save_today_stats(data, filename):
    """save load_db_stats data to a file"""
    today = datetime.datetime.now().strftime("%Y%m%d")
    if not os.path.exists(os.path.join(os.sep, "tmp", "stats.temp")):
        os.makedirs(os.path.join(os.sep, "tmp", "stats.temp"))
    file = os.path.join(os.sep, "tmp", "stats.temp", "%s%s.json" % (filename, today))
    with open(file, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, indent=4))


def load_yesterday_stats(filename):
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    file = os.path.join(os.sep, "tmp", "stats.temp", "%s%s.json" % (filename, yesterday))
    try: 
        fd = open(file, 'r', encoding='urf-8')
        stats = json.loads(fd.read())
    except FileNotFoundError:
        stats = {}
    # with open(file, "r", encoding="utf-8") as f:
    #     stats = json.loads(f.read())
    return stats


def recyclable_picture():
    global num
    TodayStats = FileBytesAvailableForReuse
    YesterdayStats = load_yesterday_stats("available.reuse")
    if not YesterdayStats:
        YesterdayStats = collection_not_exist()
        del YesterdayStats['total']
    x_label_today, y_data_today = separate_dict(TodayStats)
    x_label_yesterday, y_data_yesterday = separate_dict(YesterdayStats)
    context = {
        "ax": ax,
        "num": num,
        "title": "FileBytesAvailableForReuse",
        "unit": "GB",
        "x_label_today": x_label_today,
        "x_label_yesterday": x_label_yesterday,
        "y_data_today": y_data_today,
        "y_data_yesterday": y_data_yesterday
    }
    draw_picture(**context)


def collection_picture():
    global num
    TodayStats = load_collection_stats()
    YesterdayStats = load_yesterday_stats("collection")
    for collection in AllWeNeedCollections:
        x_label_today, y_data_today = separate_dict(TodayStats[collection])
        x_label_yesterday, y_data_yesterday = separate_dict(
            YesterdayStats.get(collection, collection_not_exist())
        )
        context = {
            "ax": ax,
            "num": num,
            "title": collection + ".storageSize",
            "unit": get_unit(collection),
            "x_label_today": x_label_today,
            "x_label_yesterday": x_label_yesterday,
            "y_data_today": y_data_today,
            "y_data_yesterday": y_data_yesterday
        }
        draw_picture(**context)
        num += 1
    # plt.show()


def stats_picture():
    global num
    TodayStats = load_dbstats()
    YesterdayStats = load_yesterday_stats("stats")
    # fig, ax = plt.subplots(nrows=5, ncols=1, figsize=(17, 15))
    for field in AllWeNeedFields:
        x_label_today, y_data_today = separate_dict(TodayStats[field])
        x_label_yesterday, y_data_yesterday = separate_dict(
            YesterdayStats.get(field, collection_not_exist())
        )
        context = {
            "ax": ax,
            "num": num, # 第几张图
            "title": "appdata." + field,
            "unit": get_unit(field),
            "x_label_today": x_label_today,
            "x_label_yesterday": x_label_yesterday,
            "y_data_today": y_data_today,
            "y_data_yesterday": y_data_yesterday
        }
        draw_picture(**context)
        num += 1
    # plt.show()

def separate_dict(stats):
    """
    separate: 分离
    分离字典的key和value
    """
    stats = sorted(stats.items(), key=lambda x: x[0])
    x_label = []
    y_data = []
    for k, v in stats:
        x_label.append(k)
        y_data.append(v)
    return x_label, y_data

def draw_picture(**kwargs):
    num = kwargs.get("num")
    today_data = kwargs.get("y_data_today")
    yesterday_data = kwargs.get("y_data_yesterday")

    ind = np.arange(len(today_data))  # the x locations for the groups
    width = 0.35  # the width of the bars
    ax = kwargs.get("ax")
    # fig, ax = plt.subplots(nrows=5, ncols=1, figsize=(17, 15))
    # font size
    plt.rc('legend', fontsize=10)
    plt.rc('xtick', labelsize=10)
    plt.rc('ytick', labelsize=10)
    plt.rc('axes', labelsize=10)
    plt.rc('axes', titlesize=10)
    plt.rc('font', size=7)
    rects1 = ax[num].bar(ind - width/2, yesterday_data, width,
                    color='IndianRed', label='yesterday')
    rects2 = ax[num].bar(ind + width/2, today_data, width,
                    color='SkyBlue', label='today')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax[num].set_ylabel(kwargs.get("unit"))
    ax[num].set_title(kwargs.get("title"))
    ax[num].set_xticks(ind)
    ax[num].set_xticklabels(kwargs.get("x_label_today"))
    ax[num].legend()


    def autolabel(rects, xpos='center'):
        """
        Attach a text label above each bar in *rects*, displaying its height.

        *xpos* indicates which side to place the text w.r.t. the center of
        the bar. It can be one of the following {'center', 'right', 'left'}.
        """

        xpos = xpos.lower()  # normalize the case of the parameter
        ha = {'center': 'center', 'right': 'left', 'left': 'right'}
        offset = {'center': 0.5, 'right': 0.57, 'left': 0.43}  # x_txt = x + w*off

        for rect in rects:
            height = rect.get_height()
            ax[num].text(rect.get_x() + rect.get_width()*offset[xpos], 1.01*height,
                    '{}'.format(height), ha=ha[xpos], va='bottom')


    autolabel(rects1, "left")
    autolabel(rects2, "right")

    # plt.show()

def main():
    stats_picture()
    collection_picture()
    recyclable_picture()
    today = datetime.datetime.now().strftime("%Y%m%d")
    file = os.path.join(os.sep, "tmp", "stats.temp", "%s%s.png" % ("mongodata", today))
    plt.subplots_adjust(hspace=0.5) # 调整每张子图之间的间距
    plt.tight_layout() # 紧凑布局, 减小页边距
    plt.savefig(file, dpi=200)


if __name__ == "__main__":
    AllShardsDict = load_all_shard()
    main()
