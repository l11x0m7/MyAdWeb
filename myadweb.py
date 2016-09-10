#-*- coding:utf-8 -*-

import sys
import os
from flask import Flask
from flask import render_template
from flask import request
from flask import make_response
from flask_script import Manager
import datetime
import mydb
import random

db = mydb.Mydb()

app = Flask(__name__)
manager = Manager(app)
_EXPIRE_TIME = datetime.datetime.strptime('20201231', '%Y%m%d')

@app.route('/')
def home():
    response = make_response('<h1>Welcome to Linxuming\'s HomePage!</h1>' + render_template('homepage.html'))
    return response

@app.route('/rec/')
def index():
    ip = request.remote_addr
    uid = request.headers.get('Cookie')
    time = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
    uids = db.getCookies()
    print '------------->', uid
    print uids

    if uid == None or (unicode(uid[4:]),) not in uids:
        if uid == None:
            uid = random.randint(1, 1e10)
            while uid in uids:
                uid = random.randint(1, 1e10)
            uid = str(uid)
        else:
            uid = uid[4:]
        user_profile = 'Unknown'
        user_sex = 'Unknown'

        ad_pre = ["skin01", "skin05", "skin09", "digital01", "digital05"]
        ad_post = ["shoes01", "shoes07", "shoes04", "shoes10", "digital09"]
        ad_all = ad_pre + ad_post
        user_behavior = [[user_profile, user_sex]]

        db.insertCtrLog(uid, user_behavior, ad_all)
        instance = [uid, ip, time, user_profile, user_sex]
        db.insertInstanceTable(instance, "visit_log")
        instance = [uid, user_profile, user_sex]
        db.insertInstanceTable(instance, "user_tag")

        response = make_response(render_template(
            'index_default.html', ad_pre = ad_pre, ad_post = ad_post
        ) + '<h1>Welcome to visit MyRec!</h1>')
        response.set_cookie('uid', uid, expires=_EXPIRE_TIME)
        return response

    else:
        user_behavior = db.getUserBehavior(uid)
        user_tag = user_behavior[0][0]
        user_sex = user_behavior[0][1]
        if user_sex == 'Unknown':
            user_sex = 'Unknown'
        elif user_sex == '1':
            user_sex = 'male'
        else:
            user_sex = 'female'

        res = db.getRank(user_behavior[0])
        if len(res) == 0:
            ad_pre = ["skin01", "skin05", "skin09", "digital01", "digital05"]
            ad_post = ["shoes01", "shoes07", "shoes04", "shoes10", "digital09"]
        else:
            ad_pre = res[0][0]
            ad_post = res[0][1]

        ad_pre = ad_pre.split(',')
        ad_post = ad_post.split(',')
        ad_all = ad_pre + ad_post
        # print ad_all
        # last_visit_time = datetime.datetime.strptime(db.getLastVisitTime(uid[4:]), '%Y%m%d %H:%M:%S')
        # cur_visit_time = datetime.datetime.strptime(time, '%Y%m%d %H:%M:%S')
        # delta = cur_visit_time - last_visit_time
        # if delta.seconds>20:
        db.insertCtrLog(uid[4:], user_behavior, ad_all)

        instance = [uid[4:], ip, time, user_tag, user_sex]
        db.insertInstanceTable(instance, "visit_log")

        addition = "<h1>Your tag is:<strong>"+user_tag+"</strong>, and your sex is: <strong>"+user_sex +"</strong></h1>"
        response = make_response(render_template('index_default.html', ad_pre = ad_pre,
            ad_post = ad_post) + addition)
        return response


@app.route('/click/<img_id>')
def click(img_id):
    from flask import redirect
    from flask import url_for
    uid = request.headers.get('Cookie')[4:]
    ad_id = img_id
    instance = [uid, ad_id]
    db.insertInstanceTable(instance, "user_click")
    db.updateCtrLog(uid, ad_id)
    print 'Recording %s done!' % img_id
    return redirect(url_for('index'))

if __name__ == '__main__':
    manager.run()
