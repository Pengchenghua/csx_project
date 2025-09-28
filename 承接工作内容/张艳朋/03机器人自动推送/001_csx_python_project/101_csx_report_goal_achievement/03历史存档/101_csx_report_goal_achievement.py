# -*- coding: utf-8 -*-
import requests
import json
import time
import ftplib
import datetime
import win32com.client as win32
from apscheduler.schedulers.blocking import BlockingScheduler
import pythoncom

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
        chat_id = res.get("chat_id")
        content_tmp="{\"text\":" + "\"" + text + "\"" + "}"
        data = {
            "receive_id": chat_id,
            "content":content_tmp,
            "msg_type": "text"
        }
        try:
            res=requests.post(self.messages_url, headers=self.headers,json=data)
            return res.json()
        except Exception as e:
            return {"error":e}

    # 发送图片消息
    def send_pic(self,image_path):
        res = self.get_chat_list()
        chat_id = res.get("chat_id")
        content_tmp="{\"image_key\":" + "\"" + self.upload_image(image_path) + "\"" + "}"
        data = {
            "receive_id": chat_id,
            "content": content_tmp,
            "msg_type": "image"
        }
        try:
            res=requests.post(self.messages_url, headers=self.headers,json=data)
            return res.json()
        except Exception as e:
            return {"error":e}

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
            return res.json()
        except Exception as e:
            return {"error":e}


def login_down_file():
    #配置参数
    local_file_path='E:/001_csx_python_project/101_csx_report_goal_achievement/01dow/'

    # FTP服务器连接信息
    ftp_server = "10.0.74.192"
    ftp_user = "ftpcsxsjz"
    ftp_password = "yhcsx123456"

    # 打开FTP连接
    ftp = ftplib.FTP(ftp_server)
    ftp.encoding='utf-8'
    ftp.login(ftp_user, ftp_password)
    print(ftp.getwelcome())

    #计算当前日期 并转化为字符串
    #current_date=datetime.date.today()
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")

    #拼接目录
    remote_directory='WEB-INF/schedule/彩食鲜业绩战报-目标达成/' + today_str + '/0900/通用结果/'

    # 进入目标目录
    #ftp.cwd('WEB-INF/schedule/彩食鲜业绩战报-目标达成/2023-09-01/0900/通用结果/')
    ftp.cwd(remote_directory)
    dir_name = ftp.nlst()[0]
    ftp.cwd('./'+dir_name)
    remote_file_name=ftp.nlst()[-1]

    # 下载Excel文件
    try:
        with open(local_file_path + remote_file_name, "wb") as f:
            ftp.retrbinary("RETR " + remote_file_name, f.write)
            print('下载成功： '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        print('error is:',e) #打印异常日志
    time.sleep(2)
    # 关闭FTP连接
    ftp.quit()

#对下载下来的excel文件截图
def excel_catch_screen(vba_code):
    pythoncom.CoInitialize()  #excel多线程相关
    excel = win32.DispatchEx('Excel.Application')
    excel.Visible = True
    excel.DisplayAlerts = False
    local_file_path='E:/001_csx_python_project/101_csx_report_goal_achievement/01dow/'
    local_file_name='彩食鲜战报报表-目标达成_'+ (datetime.date.today()-datetime.timedelta(days=1)).strftime("%Y%m%d") +'.xlsx'

    #wb = excel.Workbooks.Open('E:/01csx/04test/04down_pdf/01dow/彩食鲜战报报表-目标达成_20230831.xlsx')
    wb = excel.Workbooks.Open(local_file_path+local_file_name)
    #增加VBA模块
    module = wb.VBProject.VBComponents.Add(1)
    #代码模块对象
    code_module = module.CodeModule
    #增加代码
    code_module.AddFromString(vba_code)
    excel.Run(module.Name + '.TheMacro')
    time.sleep(2)
    excel.Quit()
    pythoncom.CoUninitialize()

vba_code = '''
Sub TheMacro()
    ActiveSheet.UsedRange.select
    ActiveSheet.UsedRange.CopyPicture
    With ActiveSheet.ChartObjects.Add(0, 0, selection.Width, selection.Height).Chart  '通过在当前工作表添加相同大小的图表的方式转存成图片
               .Parent.Select  '粘贴复制后的图片
               .Paste  '粘贴复制后的图片
               .Export "E:/001_csx_python_project/101_csx_report_goal_achievement/01dow/" & "101_csx_report_goal_achievement" & ".JPG"
               .Parent.Delete  '删除该图表
    End With
End Sub
'''

def main():
    login_down_file()
    excel_catch_screen(vba_code)
    app_id="cli_a47c6dd3ec789013"
    app_secret="vMluLBgXzDQjBSPazRTSeeW8klHwNF2b"
    chat_name="彩食鲜战报报表"
    #chat_name="消息测试1"
    messages_url="https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    fei=FeishuApi(app_id,app_secret,chat_name,messages_url)
    #res=fei.send_msg("测试")
    res=fei.send_pic('E:/001_csx_python_project/101_csx_report_goal_achievement/01dow/101_csx_report_goal_achievement.JPG')
    #res=fei.send_file("F:/01/07project/01weather/产线商品销售数据.xlsx","xlsx","产线数据")
    #res=fei.send_file('E:/01csx/04test/04down_pdf/01dow/省区实时下单业绩.xlsx',"xlsx","省区实时下单业绩")

if __name__ == '__main__':
	scheduler=BlockingScheduler()
	scheduler.add_job(main,'cron',hour='9',minute='10',coalesce=True, misfire_grace_time=300)
	scheduler.start()
	#scheduler.remove_all_jobs(jobstore=None)
	#main()