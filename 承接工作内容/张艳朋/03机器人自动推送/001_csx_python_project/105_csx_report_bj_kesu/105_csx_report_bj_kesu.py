# -*- coding: utf-8 -*-
import requests
import json
import time
import ftplib
import datetime
import win32com.client as win32
from apscheduler.schedulers.blocking import BlockingScheduler
import pythoncom
import os

class FeishuApi():
    def __init__(self,app_id,app_secret,chat_name,messages_url):
        self.app_id=app_id
        self.app_secret=app_secret
        self.chat_name=chat_name
        self.access_token=self.get_access_token()
        self.messages_url=messages_url
        self.headers={
            "Authorization": "Bearer {}".format(self.access_token),
            "Content-Type": "application/json;charset=utf-8"
        }

    # 获取token
    def get_access_token(self):
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        try:
            res = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/", json=data)
            if res.status_code == 200:
                res_json = res.json()
                access_token = res_json.get("tenant_access_token")
                return access_token
        except Exception as e:
            return {"error": e}

    # 获取群列表
    def get_chat_list(self):
        params = {
            "page_size": 100,
            "page_token": ""
        }
        try:
            res = requests.get("https://open.feishu.cn/open-apis/chat/v4/list", headers=self.headers, params=params)
            #res = requests.get("https://open.feishu.cn/open-apis/im/v1/chats",headers=self.headers,params=params,)
            if res.status_code == 200:
                res_json = res.json()
                data = res_json.get("data")
                groups = data.get("groups")
                for i in groups:
                    if i.get("name") == self.chat_name:
                        return i
        except Exception as e:
            return {"error": e}

    # 上传图片
    def upload_image(self,image_path):
        with open(image_path, 'rb') as f:
            image = f.read()
        res=requests.post("https://open.feishu.cn/open-apis/im/v1/images",
                          headers={"Authorization": "Bearer {}".format(self.access_token)},
                          files={"image": image},
                          data={"image_type": "message"},
                          stream=True)
        res.raise_for_status()
        content = res.json()
        if content.get("code") == 0:
            content=content["data"]["image_key"]
            return content
        else:
            raise Exception("Call Api Error, errorCode is %s" % content["code"])

    # 上传文件
    def upload_file(self,file_path,type,rename):
        url = "https://open.feishu.cn/open-apis/im/v1/files"
        payload={'file_type': type,
               'file_name': rename + "." + type }
        files=[
             ('file',('file',open(file_path,'rb'),'application/pdf'))
              ]
        headers={"Authorization": "Bearer {}".format(self.access_token)}
        res = requests.request("POST", url, headers=headers, data=payload, files=files)
        res.raise_for_status()
        content = res.json()
        if content.get("code") == 0:
            content=content["data"]["file_key"]
            return content
        else:
            raise Exception("Call Api Error, errorCode is %s" % content["code"])

    # 发送文本消息
    def send_msg(self,text):
        res = self.get_chat_list()
        # 从聊天列表中获取聊天ID  
        chat_id = res.get("chat_id") 
        content_tmp="{\"text\":" + "\"" + text + "\"" + "}"
        data = {
            "receive_id": chat_id,
            "content":content_tmp,
            "msg_type": "text"
        }
        try:
            res=requests.post(self.messages_url, headers=self.headers,json=data)
            message_id = res.json()['data']['message_id']
            print(message_id)
            return res.json()
        except Exception as e:
            return {"error":e}

    # 发送图片消息
    # def send_pic(self,image_path):
    #     res = self.get_chat_list()
    #     chat_id = res.get("chat_id")
    #     content_tmp="{\"image_key\":" + "\"" + self.upload_image(image_path) + "\"" + "}"
    #     data = {
    #         "receive_id": chat_id,
    #         "content": content_tmp,
    #         "msg_type": "image"
    #     }
    #     try:
    #         res=requests.post(self.messages_url, headers=self.headers,json=data)
    #         message_id = res.json()['data']['message_id']
    #         print(message_id)
    #         return res.json()
    #     except Exception as e:
    #         return {"error":e}

    # 发送文件消息
    def send_file(self,file_path,type,rename):
        res = self.get_chat_list()
        chat_id = res.get("chat_id")
        content_tmp="{\"file_key\":" + "\"" + self.upload_file(file_path,type,rename) + "\"" + "}"
        data = {
            "receive_id": chat_id,
            "content": content_tmp,
            "msg_type": "file"
        }
        try:
            res=requests.post(self.messages_url, headers=self.headers,json=data)
            message_id = res.json()['data']['message_id']
            print(message_id)
            return res.json()
        except Exception as e:
            return {"error":e}


def login_down_file():
    # FTP服务器连接信息
    ftp_server = "10.0.74.192"
    ftp_user = "ftpcsxsjz"
    ftp_password = "yhcsx123456"

    # 打开FTP连接
    ftp = ftplib.FTP(ftp_server)
    ftp.encoding='utf-8'
    ftp.login(ftp_user, ftp_password)
    print(ftp.getwelcome())

    #拼接目录
    remote_directory='WEB-INF/schedule/客诉记录表_北京当日数据/'
    # 进入目标目录
    ftp.cwd(remote_directory)
    ftp.nlst()
    dir_name_1=ftp.nlst()[-1]+'/'
    ftp.cwd('./'+dir_name_1)
    dir_name_2=ftp.nlst()[-1]+'/通用结果/'
    ftp.cwd('./'+dir_name_2)
    ftp.nlst()
    remote_file_name=ftp.nlst()[-1]

    # 下载Excel文件
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),remote_file_name) , "wb") as f:
            ftp.retrbinary("RETR " + remote_file_name, f.write)
            print('下载成功： '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        print('error is:',e) #打印异常日志
    time.sleep(2)
    # 关闭FTP连接
    ftp.quit()

#对下载下来的excel文件截图
def OpenWorkbookRunVba():
    local_file_name='宏文件.xlsm'
    pythoncom.CoInitialize()  #excel多线程相关
    excel = win32.DispatchEx('Excel.Application')
    excel.Visible = True
    excel.DisplayAlerts = False
    wb = excel.Workbooks.Open(os.path.join(os.path.dirname(os.path.abspath(__file__)),local_file_name))
    excel.Application.Run("main")
    time.sleep(2)
    wb.Close(SaveChanges=True)
    excel.Quit()
    pythoncom.CoUninitialize()

def main():
    login_down_file()
    OpenWorkbookRunVba()
    app_id="cli_a47c6dd3ec789013"
    app_secret="vMluLBgXzDQjBSPazRTSeeW8klHwNF2b"
    #chat_name="北京毛利改善工作群"
    chat_name="彭承华"
    messages_url="https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    fei=FeishuApi(app_id,app_secret,chat_name,messages_url)
    res=fei.send_msg("今日截至9点，北京客诉数据情况如下：")
    res=fei.send_pic('bj_kesu.JPG')
    res=fei.send_file(os.path.join(os.path.dirname(os.path.abspath(__file__)),"客诉记录表_北京客诉明细.xlsx"),"xlsx","客诉记录表_北京客诉明细")
if __name__ == '__main__':
	scheduler=BlockingScheduler()
	#scheduler.add_job(main,'cron',hour='9',minute='12',coalesce=True, misfire_grace_time=300)
	#scheduler.start()
	#scheduler.remove_all_jobs(jobstore=None)
	main()