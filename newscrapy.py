#coding:utf-8

from urllib.request import urlopen
from urllib.request import Request
from bs4 import BeautifulSoup
from urllib.error import HTTPError, URLError
import re
import pymysql
import time
import random

##**************************************************************************************************************
# 初始化数据库，若数据库和表不存在则创建
def init_DB():
    # 连接数据库
    db = pymysql.connect("localhost", "root", "mysql 数据库密码", "sys")
    cursor = db.cursor()
    # 执行sql语句，创造数据库
    cursor.execute("create database if not exists news_DB;")
    cursor.execute("use news_DB;")
    # 创造不同分类数据表
    plate_names = ['world', 'china', 'military', 'taiwan', 'opinion', 'finance', 'tech', 'auto', 'sports', 'smart']
    for plate_name in plate_names:
        sql_str = "create table if not exists " + plate_name + """(
        id INT NOT NULL AUTO_INCREMENT,
        news_title varchar(50) NOT NULL,
        news_url varchar(200) NOT NULL,
        news_content varchar(16000) NOT NULL,
        PRIMARY KEY (id)
        )"""
        cursor.execute(sql_str)

    db.close()


##**************************************************************************************************************
# 获取不同板块下的新闻链接
def get_news_link(url):
    try:
        headers = {
                    'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
                    'Referer': r'http://www.lagou.com/zhaopin/Python/?labelWords=label',
                    'Connection': r'keep-alive'
                 }
        req = Request(url, headers=headers)
        html = urlopen(req).read()
        html = html.decode("utf-8")
    except (HTTPError, URLError, UnicodeDecodeError) as e:
        #print(e)
        return None

    news_url_list = []
    news_title_list = []
    bsObj = BeautifulSoup(html, "lxml")
    try:
        # 寻找类似"<a href=\"(.*?)\" target=\"_blank\" title=\"(.*?)\">.*?</a>"
        for tag in bsObj.find_all("a", {"target":"_blank"}):
            news_info = re.findall(r"<a href=\"(.*?)\" target=\"_blank\" title=\"(.*?)\">.*?</a>",str(tag))
            if news_info:
                (news_url, news_title) = news_info[0]
                if(len(news_title) > 10):
                    if("index" not in news_url) and ("html" in news_url):
                        news_url_list.append(news_url)
                        news_title_list.append(news_title)
        
        # 寻找类似"<a href=\"(.*?)\" title=\"(.*?)\">.*?</a>"
        for tag in bsObj.find_all("a"):
            news_info = re.findall(r"<a href=\"(.*?)\" title=\"(.*?)\">.*?</a>",str(tag))
            if news_info:
                (news_url, news_title) = news_info[0]
                if(len(news_title) > 10):
                    if("index" not in news_url) and ("html" in news_url):
                        news_url_list.append(news_url)
                        news_title_list.append(news_title)
        
        # 寻找类似"<a href=\"(.*?)\" target=\"_blank\">(.*?)</a>"
        for tag in bsObj.find_all("a"):
            news_info = re.findall(r"<a href=\"(.*?)\" target=\"_blank\">(.*?)</a>",str(tag))
            if news_info:
                (news_url, news_title) = news_info[0]
                if((len(news_title) > 10) and (news_title != "GlobalTimes") and (news_title != "About huanqiu.com")):
                    if("index" not in news_url) and ("html" in news_url):
                        news_url_list.append(news_url)
                        news_title_list.append(news_title)
    except AttributeError as e:
        return (news_url_list, news_title_list)
    return (news_url_list, news_title_list)


##*********************************************************************************************************
# 获取网页中新闻的内容
def get_news_content(news_url):
    try:
        headers = {
                    'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
                    'Referer': r'http://www.lagou.com/zhaopin/Python/?labelWords=label',
                    'Connection': r'keep-alive'
                 }
        req = Request(news_url, headers=headers)
        html = urlopen(req).read()
        html = html.decode("utf-8")
    except (HTTPError, URLError, UnicodeDecodeError) as e:
        #print(e)
        return None

    news_content = ""
    bsObj = BeautifulSoup(html, "lxml")
    try:
        for tag in bsObj.find_all("div", {"class":"con_left"}):
            news_contents = re.findall(r"<p>(.*?)</p>",str(tag))
            news_contents = news_contents[:-1]
            for content in news_contents:
                news_content += content
            return news_content
    except AttributeError as e:
        return "No content!"

# 爬取环球网新闻：国际、国内、军事、台海、评论、财经、科技、汽车、体育、智能
if __name__ == "__main__":
    # 初始化数据库
    init_DB()
    # 环球网不同板块新闻链接
    classify_dict = {
        "国际":"http://world.huanqiu.com/article/",
        "国内":"http://china.huanqiu.com/article/",
        "军事":"http://mil.huanqiu.com/world/",
        "台海":"http://taiwan.huanqiu.com/article/",
        "评论":"http://opinion.huanqiu.com/roll_",
        "财经":"http://finance.huanqiu.com/roll/",
        "科技":"http://tech.huanqiu.com/original/",
        "国际汽车":"http://auto.huanqiu.com/globalnews/",
        "国内汽车":"http://auto.huanqiu.com/news/"
    }

    # 数据库中不同板块对应表名
    DB_dict = {
        "国际":"world",
        "国内":"china",
        "军事":"military",
        "台海":"taiwan",
        "评论":"opinion",
        "财经":"finance",
        "科技":"tech",
        "国际汽车":"auto",
        "国内汽车":"auto"
    }
    
    # 新闻计数
    news_count = 0 
    for key in classify_dict:
        for index in range(2, 30):
            # 连接数据库
            db = pymysql.connect("localhost", "root", "mysql 数据库密码", "sys")
            cursor = db.cursor()
            cursor.execute("use news_DB;")
            # 获取新闻链接
            web_url = classify_dict[key] + str(index) + ".html"
            (news_url_list, news_title_list) = get_news_link(web_url) 
            # 获取新闻内容
            if news_url_list != []:
                for i in range(len(news_url_list)):
                    news_content = get_news_content(news_url_list[i])
                    if news_content != None:
                        # 查询新闻内容是否重复
                        sql_str = "SELECT * from " + str(DB_dict[key]) + " where news_url = " + "\"" + str(news_url_list[i]) + "\";"
                        cursor.execute(sql_str)
                        results = cursor.fetchall()
                        if len(results) > 0:
                            continue

                        # 插入新闻相关信息
                        sql_str = "INSERT INTO " + str(DB_dict[key]) + "(news_title, news_url, news_content) VALUES (\"" +\
                            news_title_list[i] + "\", \"" + str(news_url_list[i]) + "\", \"" + str(news_content) + "\");"
                        #print(sql_str)
                        try:
                            cursor.execute(sql_str)
                            db.commit()
                        except Exception as e:
                            #print(e)
                            continue
                        sleep_time = random.randint(5, 15)
                        time.sleep(sleep_time)
                        news_count += 1
                        if (news_count%50 == 0):
                            print(news_count)
                    else:
                        continue
            else:
                print("Could not find news!!")

            # 关闭数据库
            cursor.close()
            db.close()
        
    

    
