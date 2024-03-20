#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

import psycopg2 
import numpy as np
import pandas as pd

from comm_update import *

if __name__ == '__main__':

    file_name='my_optional2.txt'
    df=show_realdata(file_name)
    # df = df.sort_values('hk_m_total', ascending=0)
    df = df.sort_values('a_pct', ascending=0)
    if debug:
        print(df)

    cgi_generate_html(df)
