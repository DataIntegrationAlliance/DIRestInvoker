# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
import pandas as pd
import requests
import json
import logging
from direstinvoker.utils.fh_utils import split_chunk
from direstinvoker import format_2_date_str, APIError
from simplejson.errors import JSONDecodeError

logger = logging.getLogger('ifind')


class IFinDInvoker:
    def __init__(self, url_str):
        self.url = url_str
        self.header = {'Content-Type': 'application/json','Connection': 'close'}

    def _url(self, path: str) -> str:
        return self.url + path

    def _public_post(self, path: str, req_data: str) -> list:

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

    def THS_DateSerial(self, thscode, jsonIndicator, jsonparam, globalparam, begintime, endtime, max_code_num=None) -> pd.DataFrame:
        """
        日期序列
        :param thscode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param jsonIndicator:指标，可以是单个指标也可以是多个指标，指标指标用 分号(‘;’)隔开。例如 ths_close_price_stock;ths_open_price_stock
        :param jsonparam:参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号 (‘ , ’) 隔开， 参 数 的 赋 值 用 冒 号 (‘:’) 。 例 如 100;100
        :param globalparam:参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号 (‘, ’) 隔开， 参 数 的 赋 值 用 冒 号 (‘:’) 。 例 如 Days:Tradedays,Fill:Previous,Interval:D
        :param begintime:开始时间，时间格式为 YYYY-MM-DD，例如 2018-06-24
        :param endtime:截止时间，时间格式为 YYYY-MM-DD，例如 2018-07-24
        :return:
        """
        path = 'THS_DateSerial/'
        code_list = []
        if type(thscode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list.append(','.join(thscode))
            else:
                # 如果 max_code_num 有值，自动分段切割
                for a_list in split_chunk(thscode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thscode)

        df_list = []
        try:
            for sub_list in code_list:
                req_data_dic = {"thscode": sub_list, "jsonIndicator": jsonIndicator,
                                "jsonparam": jsonparam, "globalparam": globalparam,
                                "begintime": format_2_date_str(begintime),
                                "endtime": format_2_date_str(endtime)
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                if 'errcode' in exp.ret_dic and exp.ret_dic['errcode'] == -4001:
                    logger.warning('THS_DateSerial(%s, %s, %s, %s, %s, %s, %s) 没有数据',
                                   thscode, jsonIndicator, jsonparam, globalparam, begintime, endtime, max_code_num)
                else:
                    raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_DateSerial(%s, %s, %s, %s, %s, %s, %s) 失败',
                                 thscode, jsonIndicator, jsonparam, globalparam, begintime, endtime, max_code_num)
        finally:
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_HighFrequenceSequence(self, thscode, jsonIndicator, jsonparam, begintime, endtime,max_code_num=None) -> pd.DataFrame:
        """
        高频序列
        :param thscode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param jsonIndicator:指标，可以是单个指标也可以是多个指标，指标指标用 分号(‘;’)隔开。例如 ths_close_price_stock;ths_open_price_stock
        :param jsonparam:参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号 (‘ , ’) 隔开， 参 数 的 赋 值 用 冒 号 (‘:’) 。 例 如 100;100
        :param begintime:开始时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2018-05-15 09:30:00
        :param endtime:截止时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2018-05-15 10:00:00
        :return:
        """
        path = 'THS_HighFrequenceSequence/'
        code_list = []
        if type(thscode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list = ','.join(thscode)
            else:
                for a_list in split_chunk(thscode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thscode)

        df_list = []
        try:
            for sub_list in code_list:
                req_data_dic = {"thscode": sub_list, "jsonIndicator": jsonIndicator,
                                "jsonparam": jsonparam,
                                "begintime": format_2_date_str(begintime),
                                "endtime": format_2_date_str(endtime)
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_HighFrequenceSequence(%s, %s, %s, %s, %s, %s) 失败',
                                 thscode, jsonIndicator, jsonparam, begintime, endtime, max_code_num)
        finally:
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_RealtimeQuotes(self, thscode, jsonIndicator, jsonparam="", max_code_num=None) -> pd.DataFrame:
        """
        实时序列
        :param thscode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param jsonIndicator:指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'close;open'
        :param jsonparam:参数，可以是默认参数也可以根据说明对参数进行自定义赋值，参数和参数之间用逗号(‘，’)隔开，参数的赋值用冒号(‘:’)。例如'pricetype:1'
        :return:
        """
        path = 'THS_RealtimeQuotes/'
        code_list = []
        if type(thscode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list = ','.join(thscode)
            else:
                for a_list in split_chunk(thscode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thscode)

        df_list = []
        try:
            for sub_list in code_list:
                req_data_dic = {"thscode": sub_list, "jsonIndicator": jsonIndicator,
                                "jsonparam": jsonparam
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_RealtimeQuotes(%s, %s, %s, %s) 失败',
                                 thscode, jsonIndicator, jsonparam, max_code_num)
        finally:
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_HistoryQuotes(self, thscode, jsonIndicator, jsonparam, begintime, endtime, max_code_num=None) -> pd.DataFrame:
        """
        历史序列
        :param thscode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param jsonIndicator:指标，可以是单个指标也可以是多个指标，指标指标用 分号(‘;’)隔开。例如'close;open'
        :param jsonparam:参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号(‘，’)隔开，参数的赋值用冒号(‘:’)。例如' period:D,pricetype:1,rptcategory:1'
        :param begintime:开始时间，时间格式为YYYY-MM-DD，例如2015-06-23
        :param endtime:截止时间，时间格式为YYYY-MM-DD，例如2016-06-23
        :return:
        """
        path = 'THS_HistoryQuotes/'
        code_list = []
        if type(thscode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list.append(','.join(thscode))
            else:
                # 如果 max_code_num 有值，自动分段切割
                for a_list in split_chunk(thscode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thscode)

        df_list = []
        try:
            for sub_list in code_list:
                req_data_dic = {"thscode": sub_list, "jsonIndicator": jsonIndicator,
                                "jsonparam": jsonparam,
                                "begintime": format_2_date_str(begintime),
                                "endtime": format_2_date_str(endtime)
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_HistoryQuotes(%s, %s, %s, %s, %s, %s) 失败',
                                 thscode, jsonIndicator, jsonparam, begintime, endtime, max_code_num)
        finally:
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_Snapshot(self, thscode, jsonIndicator, jsonparam, begintime, endtime, max_code_num=None) -> pd.DataFrame:
        """
        日内快照序列
        :param thscode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param jsonIndicator:指标，可以是单个指标也可以是多个指标，指标指标用 分号(‘;’)隔开。例如'close;open'
        :param jsonparam:参数，当前参数只能是dataType:Original
        :param begintime:开始时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2017-05-15 09:30:00。
        :param endtime:截止时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2017-05-15 10:00:00
        :return:
        """
        path = 'THS_Snapshot/'
        code_list = []
        if type(thscode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list.append(','.join(thscode))
            else:
                # 如果 max_code_num 有值，自动分段切割
                for a_list in split_chunk(thscode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thscode)

        df_list = []
        try:
            for sub_list in code_list:
                req_data_dic = {"thscode": sub_list, "jsonIndicator": jsonIndicator,
                                "jsonparam": jsonparam,
                                "begintime": format_2_date_str(begintime),
                                "endtime": format_2_date_str(endtime)
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_Snapshot(%s, %s, %s, %s, %s, %s) 失败',
                                 thscode, jsonIndicator, jsonparam, begintime, endtime, max_code_num)
        finally:
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_BasicData(self, thsCode, indicatorName, paramOption, max_code_num=None) -> pd.DataFrame:
        """
        基础数据序列
        :param thsCode:同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH
        :param indicatorName:指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'ths_stock_short_name_stock;ths_np_stock'
        :param paramOption:函数对应的参数，参数和参数之间用逗号(‘，’)隔开。例如';2017-12-31,100'
        :param max_code_num:最大截取数量
        :return:
        """
        path = 'THS_BasicData/'
        code_list = []
        if type(thsCode) == list:
            # 如果是 list 数据，自动 ',' 链接
            if max_code_num is None:
                code_list.append(','.join(thsCode))
            else:
                # 如果 max_code_num 有值，自动分段切割
                for a_list in split_chunk(thsCode, max_code_num):
                    code_list.append(','.join(a_list))
        else:
            code_list.append(thsCode)

        df_list = []
        try:
            for a_list in code_list:
                req_data_dic = {"thsCode": a_list, "indicatorName": indicatorName,
                                "paramOption": paramOption
                                }
                req_data = json.dumps(req_data_dic)
                json_dic = self._public_post(path, req_data)
                if json_dic is None or len(json_dic) == 0:
                    continue
                df = pd.DataFrame(json_dic)
                df_list.append(df)
        except APIError as exp:
            if len(df_list) == 0:
                raise exp from exp
            else:
                # 对于分段查询的情况，如果中途某一段产生错误（可能是流量不够）则不抛出异常，而将已查询出来的数据返回
                logger.exception('THS_BasicData(%s, %s, %s, %s) 失败',
                                 thsCode, indicatorName, paramOption, max_code_num)
        finally:
            # 合并数据
            if len(df_list) > 0:
                df = pd.concat(df_list)
            else:
                df = None
        return df

    def THS_DataPool(self, DataPoolname, paramname, FunOption) -> pd.DataFrame:
        """
        数据池序列
        :param DataPoolname:数据池名称
        :param paramname:输入参数，参数和参数之间使用分号(‘;’)隔开，如'2016-08-31;001005010'
        :param FunOption:输出参数，参数和参数之间使用逗号(‘,’)隔开，如'date:Y, security_name:Y, thscode:Y'，其中“Y”表示输出，“N”表示不输出
        :return:
        """
        path = 'THS_DataPool/'
        req_data_dic = {"DataPoolname": DataPoolname, "paramname": paramname,
                        "FunOption": FunOption
                        }
        req_data = json.dumps(req_data_dic)
        json_dic = self._public_post(path, req_data)
        # print(json_dic)
        df = pd.DataFrame(json_dic)
        return df

    def THS_EDBQuery(self, indicators, begintime, endtime) -> pd.DataFrame:
        """
        EDB序列
        :param indicators:EDB指标ID，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如'M001620326,M002822183'
        :param begintime:开始时间，时间格式为YYYY-MM-DD，例如2015-06-23
        :param endtime:截止时间，时间格式为YYYY-MM-DD，例如2016-06-23
        :return:
        """
        path = 'THS_EDBQuery/'
        req_data_dic = {"indicators": indicators,
                        "begintime": format_2_date_str(begintime),
                        "endtime": format_2_date_str(endtime)
                        }
        req_data = json.dumps(req_data_dic)
        json_dic = self._public_post(path, req_data)
        df = pd.DataFrame(json_dic)
        return df

    def THS_DateQuery(self, exchange, params, begintime, endtime) -> pd.DataFrame:
        """
        日期查询序列
        :param exchange:交易所简称。例如'SSE'
        :param begintime:开始时间，时间格式为YYYY-MM-DD，例如2018-01-01
        :param params:时间频率、日期格式、日期类型，例如：'dateType:0,period:D,dateFormat:0'
        :param endtime:截止时间，时间格式为YYYY-MM-DD，例如2018-06-23
        :return:
        """
        path = 'THS_DateQuery/'
        req_data_dic = {"exchange": exchange,
                        "params": params,
                        "begintime": format_2_date_str(begintime),
                        "endtime": format_2_date_str(endtime)
                        }
        req_data = json.dumps(req_data_dic)
        json_dic = self._public_post(path, req_data)
        df = pd.DataFrame(json_dic)
        return df


if __name__ == "__main__":
    # url_str = "http://10.0.5.65:5000/wind/"
    url_str = "http://localhost:5000/iFind/"
    invoker = IFinDInvoker(url_str)

    try:
        data_df = invoker.THS_DateSerial('300033.SZ,600000.SH','ths_stock_short_name_stock;ths_np_stock',';100','Days:Tradedays,Fill:Previous,Interval:D','2017-12-28','2018-01-04')
        print(data_df)
    except APIError as exp:
        if exp.status == 500:
            print('APIError.status:', exp.status, exp.ret_dic['message'])
        else:
            print(exp.ret_dic.setdefault('error_code', ''), exp.ret_dic['message'])
    # date_str = rest.tdaysoffset(1, '2017-3-31')
    # print(date_str)
