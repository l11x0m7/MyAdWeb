#!/usr/bin/env python
# coding=utf-8


import MySQLdb
import logging
import pickle
import sys
logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                stream=sys.stdout)

class Mydb:
    def __init__(self):
        self.logger = logging.log
        pass

    def __getConn(self):
        return MySQLdb.connect(host='localhost',
                           user='root',
                           db = 'myrec',
                           port = 3306,
                           charset='utf8')

    def insertInstanceTable(self, instance, table):
        conn = self.__getConn()
        cur = conn.cursor()
        if table == 'visit_log':
            # uid, ip, time, user_profile, user_sex
            try:
                cur.execute('INSERT INTO visit_log VALUES (%s, %s, %s, %s, %s)', instance)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                msg = '[ERROR] insertInstanceTable %s, %s' % (table, e)
                print msg
                # self.logger(level=logging.ERROR, msg=msg)

        elif table == 'user_tag':
            # uid, user_profile, user_sex
            try:
                cur.execute('INSERT INTO user_tag VALUES (%s, %s, %s)', instance)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                msg = '[ERROR] insertInstanceTable %s, %s' % (table, e)
                print msg
                # self.logger(level=logging.ERROR, msg=msg)

        elif table == 'user_click':
            try:
                cur.execute('INSERT INTO user_click VALUES (%s, %s)', instance)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                msg = '[ERROR] insertInstanceTable %s, %s' % (table, e)
                print msg


    def insertCtrLog(self, uid, user_behavior, ad_set):
        conn = self.__getConn()
        cur = conn.cursor()
        query = "SELECT count(*) FROM ctr_log WHERE uid = '" + uid + "'"
        cur.execute(query)
        conn.commit()
        num = int(cur.fetchall()[0][0])
        ad_info = self.getAdInfo('all')
        dict_ad_info = dict(zip(zip(*ad_info)[0], zip(*zip(*ad_info)[1:])))
        # print dict_ad_info
        #feature:click, ad_id, position, advertiser_id, price, ad_tag, user_tag, user_sex, user_id
        if num == 0:
            cnt_pos = 0
            for ad_id in ad_set:
                pos = 1 if cnt_pos < 5 else 2
                #1.copy ad_id, advertiser_id, price, ad_tag from ad_info
                position = str(pos)
                advertiser_id = str(dict_ad_info[ad_id][0])
                price = str(dict_ad_info[ad_id][4])
                ad_tag = str(dict_ad_info[ad_id][3])
                user_tag = user_behavior[0][0]
                user_sex = user_behavior[0][1]
                user_id = uid
                instance = [0, ad_id, position, advertiser_id, price, ad_tag, user_tag, user_sex, user_id]

                try:
                    cur.execute("INSERT INTO ctr_log VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", instance)
                except Exception as e:
                    msg = '[ERROR] insertCtrLog, %s' % e
                    print msg
                    # self.logger(level=logging.ERROR, msg=msg)
                cnt_pos += 1
            conn.commit()
            conn.close()
        else:
            query = "SELECT ad_id FROM ctr_log WHERE uid = '" + uid + "'"
            cur.execute(query)
            conn.commit()
            ad_already_in = cur.fetchall()
            ad_already_in = [i[0] for i in ad_already_in]
            cnt_pos = 0
            for ad_id in ad_set:
                if ad_id in ad_already_in:
                    pass
                else:
                    pos = 1 if cnt_pos < 5 else 2
                    #1.copy ad_id, advertiser_id, price, ad_tag from ad_info
                    position = str(pos)
                    advertiser_id = str(dict_ad_info[ad_id][0])
                    price = str(dict_ad_info[ad_id][4])
                    ad_tag = str(dict_ad_info[ad_id][3])
                    user_tag = str(user_behavior[0][0])
                    user_sex = str(user_behavior[0][1])
                    user_id = str(uid)
                    instance = [0, str(ad_id), position, advertiser_id, price, ad_tag, user_tag, user_sex, user_id]

                    try:
                        cur.execute("INSERT INTO ctr_log VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", instance)
                    except Exception as e:
                        msg = '[ERROR] insertCtrLog, %s' % e
                        print msg
                        # self.logger(level=logging.ERROR, msg=msg)
                    cnt_pos += 1
            conn.commit()
            cur.close()
            conn.close()

    def insertAdInfo(self, ad_dict):
        # ad_dict:ad_id, advertiser_id, path, adsex, adtag, price
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            for ad_id in ad_dict:
                ad_id = ad_dict[ad_id][0]
                advertiser_id = ad_dict[ad_id][1]
                path = ad_dict[ad_id][2]
                adsex = ad_dict[ad_id][3]
                adtag = ad_dict[ad_id][4]
                price = ad_dict[ad_id][5]

                query = 'INSERT INTO ad_info VALUES (%s, %s, %s, %s, %s, %s)'
                cur.execute(query, (ad_id, advertiser_id, path, adsex, adtag, price))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            msg = '[Error] insertAdInfo, %s' % e
            print msg

    def getLastVisitTime(self, uid):
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            query = "SELECT MAX(time) FROM visit_log WHERE uid=" + uid + ";"
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[ERROR] getLastVisitTime, %s' % e
            print msg
            # self.logger(level=logging.ERROR, msg=msg)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return str(res[0][0])

    def getCookies(self):
        conn = self.__getConn()
        cur = conn.cursor()
        query = 'SELECT DISTINCT uid FROM visit_log;'

        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[Error] getCookies, %s' % e
            print msg
            # self.logger(level=logging.ERROR, msg=msg)

        uids = cur.fetchall()
        cur.close()
        conn.close()

        return uids

    def getAdInfo(self, ad_id):
        conn = self.__getConn()
        cur = conn.cursor()
        if ad_id != 'all':
            query = "SELECT * FROM ad_info WHERE ad_id=" + ad_id
        else:
            query = "SELECT * FROM ad_info;"
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[Error] getAdInfo, %s' % e
            print msg
            cur.close()
            conn.close()
            return None
            # self.logger(level=logging.ERROR, msg=msg)
        ads = cur.fetchall()
        cur.close()
        conn.close()
        return ads

    def getUserClick(self, uid):
        conn = self.__getConn()
        cur = conn.cursor()
        if uid != 'all':
            query = "SELECT * FROM user_click WHERE uid=" + uid
        else:
            query = "SELECT * FROM user_click;"
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[Error] getUserClick, %s' % e
            print msg
            cur.close()
            conn.close()
            return None
            # self.logger(level=logging.ERROR, msg=msg)
        clicks = cur.fetchall()
        cur.close()
        conn.close()
        return clicks


    def getUserBehavior(self, uid):
        conn = self.__getConn()
        cur = conn.cursor()
        if uid != "all" and uid != 'allbehave':
            query = "SELECT tag, sex FROM user_tag WHERE uid = " + uid[4:]
        elif uid == 'all':
            query = "SELECT * FROM user_tag;"
        elif uid == 'allbehave':
            query = "SELECT tag, sex FROM user_tag;"
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[Error] getUserBehavior, %s' % e
            print msg
            # self.logger(level=logging.ERROR, msg=msg)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def getCtrLog(self, uid):
        conn = self.__getConn()
        cur = conn.cursor()
        if uid != "all":
            query = "SELECT * FROM ctr_log WHERE uid = " + uid
        else:
            query = "SELECT * FROM ctr_log;"
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            msg = '[Error] getCtrLog, %s' % e
            print msg
            # self.logger(level=logging.ERROR, msg=msg)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def updateUserBehavior(self, user_behavior, table):
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            if table == 'user_tag':
                for uid in user_behavior:
                    query = "UPDATE user_tag SET tag=%s, sex=%s WHERE uid=%s;"
                    cur.execute(query, (user_behavior[uid][0], user_behavior[uid][1], uid))
            elif table == 'ctr_log':
                for uid in user_behavior:
                    query = "UPDATE ctr_log SET user_tag=%s, user_sex=%s WHERE uid=%s;"
                    cur.execute(query, (user_behavior[uid][0], user_behavior[uid][1], uid))
            conn.commit()
        except Exception as e:
            msg = '[Error] updateUserBehavior, %s' % e
            print msg
        cur.close()
        conn.close()



    def updateCtrLog(self, uid, ad_id):
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            # query "SELECT COUNT(*) FROM ctr_log WHERE uid=%s AND ad_id=%s;"
            # cur.execute(query, (uid, ad_id))
            # num = cur.fetchall()[0][0]
            query = "UPDATE ctr_log SET click=click+1 WHERE uid=%s AND ad_id=%s;"
            cur.execute(query, (uid, ad_id))
            conn.commit()
        except Exception as e:
            msg = '[Error] update_ctr_log, %s' % e
            print msg
        cur.close()
        conn.close()

    def updateRank(self, user_behavior, pre_ad, post_ad):
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            query = "SELECT COUNT(*) FROM behave_rank WHERE user_tag=%s AND user_sex=%s;"
            cur.execute(query, (user_behavior[0], user_behavior[1]))
            conn.commit()
            res = int(cur.fetchall()[0][0])
            if res==0:
                query = "INSERT INTO behave_rank VALUES (%s, %s, %s, %s);"
                cur.execute(query, (user_behavior[0], user_behavior[1], pre_ad, post_ad))
            else:
                query = "UPDATE behave_rank SET pre_ad=%s, post_ad=%s WHERE user_tag=%s AND user_sex=%s;"
                cur.execute(query, (pre_ad, post_ad, user_behavior[0], user_behavior[1]))
            conn.commit()
        except Exception as e:
            msg = '[Error] updateRank, %s' %e
            print msg

        cur.close()
        conn.close()

    def getRank(self, user_behavior):
        conn = self.__getConn()
        cur = conn.cursor()
        try:
            query = "SELECT pre_ad, post_ad FROM behave_rank WHERE user_tag=%s AND user_sex=%s;"
            cur.execute(query, (user_behavior[0], user_behavior[1]))
            conn.commit()
        except Exception as e:
            msg = '[Error] getRank, %s' %e
            print msg

        res = cur.fetchall()
        cur.close()
        conn.close()
        return res




    def clear(self, table=None):
        conn = self.__getConn()
        cur = conn.cursor()
        if table == None or table == 'all':
            try:
                query = 'DELETE FROM ctr_log;DELETE FROM user_log;DELETE FROM visit_log;'
                cur.execute(query)
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                msg = '[Error] clear, %s' % e
                print msg
                # self.logger(level=logging.ERROR, msg=msg)

        else:
            try:
                cur.execute('DELETE FROM %s', table)
                cur.execute()
                conn.commit()
                cur.close()
                conn.close()
            except Exception as e:
                msg = '[Error] clear, %s' % e
                print msg
                # self.logger(level=logging.ERROR, msg=msg)
