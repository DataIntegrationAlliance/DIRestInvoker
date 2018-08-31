# DIRestInvoker 
[![Build Status](https://travis-ci.org/DataIntegrationAlliance/DIRestInvoker.svg?branch=master)](https://travis-ci.org/DataIntegrationAlliance/DIRestInvoker)
[![GitHub issues](https://img.shields.io/github/issues/DataIntegrationAlliance/DIRestInvoker.svg)](https://github.com/DataIntegrationAlliance/DIRestInvoker/issues)
[![GitHub forks](https://img.shields.io/github/forks/DataIntegrationAlliance/DIRestInvoker.svg)](https://github.com/DataIntegrationAlliance/DIRestInvoker/network)
[![GitHub stars](https://img.shields.io/github/stars/DataIntegrationAlliance/DIRestInvoker.svg)](https://github.com/DataIntegrationAlliance/DIRestInvoker/stargazers)
[![GitHub license](https://img.shields.io/github/license/DataIntegrationAlliance/DIRestInvoker.svg)](https://github.com/DataIntegrationAlliance/DIRestInvoker/blob/master/LICENSE)
[![HitCount](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/DIRestInvoker.svg)](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/DIRestInvoker)
[![Pypi](https://img.shields.io/badge/pypi-wheel-blue.svg)](https://pypi.org/project/DIRestInvoker/)
[![Twitter](https://img.shields.io/twitter/url/https/github.com/DataIntegrationAlliance/DIRestInvoker.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2FDataIntegrationAlliance%2FDIRestInvoker)


Data Integration RESTPlus Invoker，调用 [DIRestPlus](https://github.com/DataIntegrationAlliance/DIRestPlus) 接口，实现Wind、iFinD、Choice接口调用

## 安装
```commandline
pip install DIRestInvoker
```

## iFinD接口调用举例
```python
from direstinvoker.ifind import IFinDInvoker, APIError
url_str = "http://localhost:5000/iFind/"
invoker = IFinDInvoker(url_str)
```

### THS_DateQuery
```python
try:
    data_df = invoker.THS_DateQuery('SSE', 'dateType:0,period:D,dateFormat:0', '2018-06-15', '2018-06-21')
    print(data_df)
except APIError as exp:
    if exp.status == 500:
        print('APIError.status:', exp.status, exp.ret_dic['message'])
    else:
        print(exp.ret_dic.setdefault('error_code', ''), exp.ret_dic['message'])
```

## 修改历史

* version 0.1.4

  > THS_BasicData 支持 list 作为 thsCode 参数，并且支持按一定数量自动分割发送（解决大批量数据请求的情况下，20W数字限制，可能引发-205错误的问题）

----
项目地址：[DIRestInvoker](https://github.com/DataIntegrationAlliance/DIRestInvoker)
