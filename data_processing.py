#coding:utf-8

import pymysql
import re
import jieba.analyse

def word_segmentation(stop_word_path):
    # 连接数据库
    conn = pymysql.connect(host='127.0.0.1', user='root', passwd='mysql 数据库密码', db = 'news_DB', charset = "utf8")
    cursor = conn.cursor()
    # 使用分词数据库sql语句
    sql_str = 'use news_DB;'
    cursor.execute(sql_str)
    conn.commit()

    # 验证集比例
    validation_ration = 0.16
    # 测试集比例
    test_ratio = 0.2
    # 分词数据文件输出流
    train_data_f = open('train_data.txt', 'a')
    train_label_f = open('train_label.txt', 'a')
    validation_data_f = open("validation_data.txt", 'a')
    validation_label_f = open("validation_label.txt", "a")
    test_data_f = open('test_data.txt', 'a')
    test_label_f = open('test_label.txt', 'a')
    # 设置停用词表
    jieba.analyse.set_stop_words(stop_word_path)
    #从不同板块中提取新闻内容并进行分词存入文档中
#    plate_names = {'world':0, 'china':1, 'military':2, 'taiwan':3, 'opinion':4, 'finance':5, 'tech':6, 'auto':7, 'sports':8, 'smart':9}
#    plate_names = {'world':0, 'china':1, 'military':2, 'taiwan':3, 'opinion':4, 'finance':5, 'tech':6, 'auto':7}
    plate_names = {'world':0, 'china':1, 'military':2, 'finance':3, 'tech':4, 'auto':5}
    for plate_name in plate_names.keys():
        # 从数据库中去除内容不为空的新闻标题和新闻内容
        sql_str = "select news_title from news_DB." + plate_name + " where news_content != \"\";"
        cursor.execute(sql_str)
        news_title_list = cursor.fetchall()
        sql_str = "select news_content from news_DB." + plate_name + " where news_content != \"\";"
        cursor.execute(sql_str)
        news_content_list = cursor.fetchall()
        news_num = len(news_title_list)
#        news_num = 800

        # 写入训练分词数据
        for i in range(int(news_num*(1-validation_ration-test_ratio))):
            news_str = news_title_list[i].__str__() + "。" + news_content_list[i].__str__()
            # 正则匹配,去除简介中多余字符
            pattern = "[\\a-z<>/\'&,()]"
            news_str = re.sub(pattern, "", news_str)
            train_label_f.write(str(plate_names[plate_name]))
            # 提取新闻中关键词并存入数据文档中
            for seg in jieba.analyse.extract_tags(news_str, topK=50):
                train_data_f.write(seg + " ")
            if ((i+1) == int(news_num*(1-validation_ration-test_ratio))) and (plate_name == 'auto'):
                continue
            else:
                train_data_f.write('\n')
                train_label_f.write('\n')

        # 写入验证分词数据
        for i in range(int(news_num*(1-validation_ration-test_ratio)), int(news_num*(1-test_ratio))):
            news_str = news_title_list[i].__str__() + "。" + news_content_list[i].__str__()
            # 正则匹配,去除简介中多余字符
            pattern = "[\\a-z<>/\'&,()]"
            news_str = re.sub(pattern, "", news_str)
            validation_label_f.write(str(plate_names[plate_name]))
            # 提取新闻中关键词并存入数据文档中
            for seg in jieba.analyse.extract_tags(news_str, topK=50):
                validation_data_f.write(seg + " ")
            if ((i+1) == int(news_num*(1-test_ratio))) and (plate_name == 'auto'):
                continue
            else:
                validation_data_f.write("\n")
                validation_label_f.write("\n")

        # 写入测试分词数据
        for i in range(int(news_num*(1-test_ratio)), news_num):
            news_str = news_title_list[i].__str__() + "。" + news_content_list[i].__str__()
            # 正则匹配,去除简介中多余字符
            pattern = "[\\a-z<>/\'&,()]"
            news_str = re.sub(pattern, "", news_str)
            test_label_f.write(str(plate_names[plate_name]))
            # 提取新闻中关键词并存入数据文档中
            for seg in jieba.analyse.extract_tags(news_str, topK=50):
                test_data_f.write(seg + " ")
            if ((i+1) == news_num) and (plate_name == 'auto'):
                continue
            else:
                test_data_f.write('\n')
                test_label_f.write('\n') 
    # 关闭分词数据文件输出流
    test_label_f.close()
    test_data_f.close()
    validation_label_f.close()
    validation_data_f.close()
    train_data_f.close()
    train_label_f.close()


if __name__ == "__main__":
    word_segmentation("stop_words.txt")

