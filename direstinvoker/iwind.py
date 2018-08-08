# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
import pandas as pd
import requests
import json
from datetime import datetime, date

STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00.005000
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()


def format_2_date_str(dt) -> str:
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    else:
        return dt


def format_2_datetime_str(dt) -> str:
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATETIME_WIND)
        else:
            return None
    else:
        return dt


class APIError(Exception):
    def __init__(self, status, ret_dic):
        self.status = status
        self.ret_dic = ret_dic

    def __str__(self):
        return "APIError:status=POST / {} {}".format(self.status, self.ret_dic)


class WindRestInvoker:
    def __init__(self, url_str):
        self.url = url_str
        self.header = {'Content-Type': 'application/json'}

    def _url(self, path: str) -> str:
        return self.url + path

    def public_post(self, path: str, req_data: str) -> list:

        # print('self._url(path):', self._url(path))
        ret_data = requests.post(self._url(path), data=req_data, headers=self.header)
        ret_dic = ret_data.json()

        if ret_data.status_code != 200:
            raise APIError(ret_data.status_code, ret_dic)
        else:
            return ret_dic

    def wset(self, tablename, options) -> pd.DataFrame:
        """
        获取板块、指数等成分数据
        :param tablename:数据集名称
        :param options:可选参数
        :return:
        """
        path = 'wset/'
        req_data_dic = {"tablename": tablename, "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wss(self, codes, fields, options="") -> pd.DataFrame:
        """
        获历史截面数据
        :param codes:数据集名称
        :param fields:指标
        :param options:可选参数
        :return:
        """
        path = 'wss/'
        req_data_dic = {"codes": codes, "fields": fields, "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsd(self, codes, fields, beginTime, endTime, options="") -> pd.DataFrame:
        """
        获取历史序列数据
        :param codes:数据集名称
        :param fields:指标
        :param beginTime:开始时间
        :param endTime:结束时间
        :param options:可选参数
        :return:
        """
        path = 'wsd/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "beginTime": format_2_date_str(beginTime),
                        "endTime": format_2_date_str(endTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsi(self, codes, fields, beginTime, endTime, options="") -> pd.DataFrame:
        """
        获取分钟数据数据
        :param codes:数据集名称
        :param fields:指标
        :param beginTime:开始时间
        :param endTime:结束时间
        :param options:可选参数
        :return:
        """
        path = 'wsi/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "beginTime": format_2_date_str(beginTime),
                        "endTime": format_2_date_str(endTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wst(self, codes, fields, beginTime, endTime, options="") -> pd.DataFrame:
        """
        获取日内tick级别数据
        :param codes:数据集名称
        :param fields:指标
        :param beginTime:开始时间
        :param endTime:结束时间
        :param options:可选参数
        :return:
        """
        path = 'wst/'
        req_data_dic = {"codes": codes, "fields": fields,
                        "beginTime": format_2_datetime_str(beginTime),
                        "endTime": format_2_datetime_str(endTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def wsq(self, codes, fields, options="") -> pd.DataFrame:
        """
        获取和订阅实时行情数据
        :param codes:数据集名称
        :param fields:指标
        :param options:可选参数
        :return:
        """
        path = 'wsq/'
        req_data_dic = {"codes": codes, "fields": fields, "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df

    def tdaysoffset(self, offset, beginTime, options="") -> dict:
        """
        获取某个偏移值对应的日期
        :param offset:偏移值
        :param beginTime:基准日
        :param options:可选参数
        :return:
        """
        path = 'tdaysoffset/'
        req_data_dic = {"offset": offset,
                        "beginTime": format_2_date_str(beginTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        date_str = json_dic['Date']
        return date_str

    def tdays(self, beginTime, endTime, options="") -> dict:
        """
        获取区间内的日期序列
        :param beginTime:开始时间
        :param endTime:结束时间
        :param options:可选参数
        :return:
        """
        path = 'tdays/'
        req_data_dic = {"beginTime": format_2_date_str(beginTime),
                        "endTime": format_2_date_str(endTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        # df = pd.DataFrame(json_dic)
        return json_dic

    def edb(self, codes, beginTime, endTime, options) -> pd.DataFrame:
        """
        获取EDB序列
        :param codes:数据集名称
        :param beginTime:开始时间
        :param endTime:结束时间
        :param options:可选参数
        :return:
        """
        path = 'edb/'
        req_data_dic = {"codes": codes,
                        "beginTime": format_2_date_str(beginTime),
                        "endTime": format_2_date_str(endTime),
                        "options": options}
        req_data = json.dumps(req_data_dic)
        json_dic = self.public_post(path, req_data)
        df = pd.DataFrame(json_dic).T
        return df


if __name__ == "__main__":
    # url_str = "http://10.0.5.65:5000/wind/"
    url_str = "http://localhost:5000/wind/"
    invoker = WindRestInvoker(url_str)
    # data_df = invoker.wset(tablename="sectorconstituent", options="date=2017-03-21;sectorid=1000023121000000")
    # data_df = invoker.wss(codes="QHZG160525.OF", fields="fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_ptmyear,fund_type,fund_fundmanager")
    # data_df = invoker.wsd("601398.SH", "open,high,low,close,volume", "2017-01-04", "2017-02-28", "PriceAdj=F")
    # data_df = invoker.tdays(begin_time="2017-01-04", end_time="2017-02-28")
    # data_df = invoker.wst("600000.SH", "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last", "2017-10-20 09:15:00", "2017-10-20 09:26:00", "")
    # data_df = invoker.wsi("RU1801.SHF", "open,high,low,close,volume,amt,oi", "2017-12-8 09:00:00", "2017-12-8 11:30:00", "")

    try:
        data_df = invoker.wset(tablename="sectorconstituent", options="date=2017-03-21;sectorid=1000023121000000")
        print(data_df)
    except APIError as exp:
        if exp.status == 500:
            print('APIError.status:', exp.status, exp.ret_dic['message'])
        else:
            print(exp.ret_dic.setdefault('error_code', ''), exp.ret_dic['message'])
    # date_str = invoker.tdaysoffset(1, '2017-3-31')
    # print(date_str)
