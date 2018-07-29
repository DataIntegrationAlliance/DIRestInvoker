# DIRestInvoker
调用 [DIRestPlus](https://github.com/DataIntegrationAlliance/DIRestPlus) 接口，实现Wind、iFinD、Choice接口调用

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