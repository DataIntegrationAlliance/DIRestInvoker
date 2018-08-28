# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
import pandas as pd
import requests
import json
import logging
from direstinvoker import format_2_date_str, format_2_datetime_str, APIError
from simplejson.errors import JSONDecodeError

logger = logging.getLogger('wind')


class WindRestInvoker:
    def __init__(self, url_str):
        self.url = url_str
        self.header = {'Content-Type': 'application/json'}

    def _url(self, path: str) -> str:
        return self.url + path

    def public_post(self, path: str, req_data: str) -> list:

        # print('self._url(path):', self._url(path))
        ret_data = requests.post(self._url(path), data=req_data, headers=self.header)
        try:
            ret_dic = ret_data.json()
        except JSONDecodeError:
            logger.exception('%s post %s got error\n', self._url(path), req_data)
            ret_dic = None

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
        if isinstance(codes, list):
            codes = ','.join(codes)
        if isinstance(fields, list):
            fields = ','.join(fields)
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
        if isinstance(codes, list):
            codes = ','.join(codes)
        if isinstance(fields, list):
            fields = ','.join(fields)
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
        if isinstance(codes, list):
            codes = ','.join(codes)
        if isinstance(fields, list):
            fields = ','.join(fields)
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
        if isinstance(codes, list):
            codes = ','.join(codes)
        if isinstance(fields, list):
            fields = ','.join(fields)
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
        if isinstance(codes, list):
            codes = ','.join(codes)
        if isinstance(fields, list):
            fields = ','.join(fields)
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
        if isinstance(codes, list):
            codes = ','.join(codes)
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
    url_str = "http://10.0.3.66:5000/wind/"
    invoker = WindRestInvoker(url_str)
    # data_df = invoker.wset(tablename="sectorconstituent", options="date=2017-03-21;sectorid=1000023121000000")
    # data_df = invoker.wss(codes=["601398.SH", "600123.SH"], fields="sec_name,trade_code,ipo_date,delist_date,mkt,exch_city,exch_eng,prename")
    # data_df = invoker.wsd("601398.SH", "open,high,low,close,volume", "2017-01-04", "2017-02-28", "PriceAdj=F")
    # data_df = invoker.tdays(begin_time="2017-01-04", end_time="2017-02-28")
    # data_df = invoker.wst("600000.SH", "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last", "2017-10-20 09:15:00", "2017-10-20 09:26:00", "")
    # data_df = invoker.wsi("RU1801.SHF", "open,high,low,close,volume,amt,oi", "2017-12-8 09:00:00", "2017-12-8 11:30:00", "")
    # data = invoker.edb('M0017126,M0017127,M0017128', '2016-11-10', '2018-11-10', "Fill=Previous")

    try:
        data_df = invoker.wsd("0382.HK", "open,high,low,close,adjfactor,volume,amt,pct_chg,maxupordown,swing,turn,free_turn,trade_status,susp_days,total_shares,free_float_shares,ev2_to_ebitda,ps_ttm,pe_ttm,pb_mrq", "2007-02-17", "2007-02-23", None)
        print(data_df)
    except APIError as exp:
        if exp.status == 500:
            print('APIError.status:', exp.status, exp.ret_dic['message'])
        else:
            print(exp.ret_dic.setdefault('error_code', ''), exp.ret_dic['message'])

        logger.exception("执行异常")
        if exp.ret_dic.setdefault('error_code', 0) in (
                -40520007,  # 没有可用数据
                -40521009,  # 数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月
        ):
            pass
    # date_str = invoker.tdaysoffset(1, '2017-3-31')
    # print(date_str)
