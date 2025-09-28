import requests
import json
import time
import ftplib
import datetime
import win32com.client as win32
from apscheduler.schedulers.blocking import BlockingScheduler
import pythoncom
import os


# import pandas as pd

class FeishuApiException(Exception):
    pass


class FeishuApi:

    def __init__(self, app_id, app_secret, chat_name, messages_url):
        self.app_id = app_id
        self.app_secret = app_secret
        self.chat_name = chat_name
        self.messages_url = messages_url
        self.headers = {}  # 初始化时不设置headers，因为还没有access_token  

    def get_access_token(self):
        # ... 省略了与之前的实现相同的部分 ...  
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        try:
            # ...  
            if res.status_code == 200:
                # ...  
                access_token = res_json.get("tenant_access_token")
                self.headers = {
                    "Authorization": "Bearer {}".format(access_token),
                    "Content-Type": "application/json;charset=utf-8"
                }
                return access_token
            else:
                raise FeishuApiException("Failed to get access token: {}".format(res.text))
        except Exception as e:
            raise FeishuApiException("Error getting access token: {}".format(e))

    def get_chat_list(self, res):
        # ... 省略了与之前的实现相同的部分 ... 
        params = {
            "page_size": 100,
            "page_token": ""
        }
        try:
            # 假设我们已经有了access_token（可以在类的其他方法或外部调用中设置）  
            if not self.headers or "Authorization" not in self.headers:
                raise FeishuApiException("Access token not set")

                # ...
            if res.status_code == 200:
                # ...  
                data = res_json.get("data")
                groups = data.get("groups", [])
                for group in groups:
                    if group.get("name") == self.chat_name:
                        return group
                return None  # 如果没有找到匹配的群，返回None  
            else:
                raise FeishuApiException("Failed to get chat list: {}".format(res.text))
        except Exception as e:
            raise FeishuApiException("Error getting chat list: {}".format(e))
