#!/usr/bin/env python
# coding=utf-8

import sys
import mydb
db = mydb.Mydb()
"""
    受众定向：
    此处我们只对用户的性别和偏好进行规则判定。
"""

# 利用规则对用户进行分析
def usertag():
    # 载入广告信息
    ad_info = db.getAdInfo('all')
    dict_ad_info = dict()
    for info in ad_info:
        # ad_id对应advertise_id, adsex, adtag
        dict_ad_info[info[0]] = [info[1], info[3], info[4]]

    # 载入点击信息
    click_info = db.getUserClick('all')
    dict_click_info = dict()
    for uid, ad_id in click_info:
        dict_click_info.setdefault(uid, [])
        dict_click_info[uid].append(ad_id)

    # 载入ctr信息
    ctr_info = db.getCtrLog('all')
    dict_ctr_info = dict()
    for info in ctr_info:
        uid = info[8]
        ad_id = info[1]
        click = info[0]
        dict_ctr_info.setdefault(uid, dict())
        dict_ctr_info[uid][ad_id] = click

    # 对每个用户进行受众定向
    users_info = db.getUserBehavior('all')
    new_users_info = dict()
    for user in users_info:
        uid = user[0]
        if not dict_click_info.has_key(uid):
            continue
        # tag = user[1]
        # sex = user[2]
        # 化妆品, 数码产品, 鞋子
        # skin, digital, shoes
        ad_ind = {'skin':0, 'digital':1, 'shoes':2}
        ads = ('skin', 'digital', 'shoes')
        tag = [0, 0, 0]
        sex = 0
        click_ads = dict_click_info[uid]
        for ad_id in click_ads:
            click = int(dict_ctr_info[uid][ad_id])
            adsex = dict_ad_info[ad_id][1]
            adtag = dict_ad_info[ad_id][2]
            tag[ad_ind[adtag]] += click
            sex += click if adsex == '1' else -click
        if sex>0:
            sex = '1'
        elif sex<0:
            sex = '0'
        else:
            sex = 'Unknown'
        tag = ads[tag.index(max(tag))]
        if max(tag)==min(tag):
            tag = 'Unknown'
        new_users_info[uid] = (tag, sex)
        # print uid, ":", tag, sex
    db.updateUserBehavior(new_users_info, 'user_tag')
    db.updateUserBehavior(new_users_info, 'ctr_log')







if __name__ == '__main__':
    usertag()