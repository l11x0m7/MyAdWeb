#!/usr/bin/env python
# coding=utf-8

import time
import usertag
import ctr

if __name__ == '__main__':
    while True:
        usertag.usertag()
        myctr = ctr.CTR()
        myctr.userBehaviorCTR()
        print 'UPDATE FINISHED!'
        time.sleep(3)