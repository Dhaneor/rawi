#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration file basic settings for ZeroMQ components.

Created on Fri Oct 06 21:41:23 2023

@author_ dhaneor
"""
DEV_ENV = True

COLLECTOR_KEYS = (
    'L>&NKg9E/Cxv)nw]rXl<mgy!!w:9%s($@=Fk#DDP',
    '5sIbhID73=!fbqYBeiipw)9p0?(ix#SG]SSN$KqJ'
)

collector_sub = "tcp://127.0.0.1:5582"
collector_pub = "tcp://127.0.0.1:5583"
collector_mgmt = "tcp://127.0.0.1:5570"
collector_hb = "tcp://127.0.0.1:5580"

ohlcv_repo_req = "inproc://ohlcv_repository"

STREAMER_BASE_PORT = 5500
