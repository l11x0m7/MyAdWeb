#!/usr/bin/env python
# coding=utf-8

import os
import sys
import pickle
import mydb
db = mydb.Mydb()

def parsepickle(filepath):
    with open(filepath, 'rb') as fr:
        ad_dict = pickle.load(fr)
        for ad in ad_dict:
            ad_dict[ad] = list(ad_dict[ad])
            ad_dict[ad][2] = 'static/ads/' + ad_dict[ad][4] + '/' + ad_dict[ad][0]
            print ad, ad_dict[ad]
        db.insertAdInfo(ad_dict)




if __name__ == '__main__':
    parsepickle('pkl/ad_info.pkl')