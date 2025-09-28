# -*- coding: utf-8 -*-
# 昨日采购订单稽核-福州
# import sys
import requests
# import json
import time
import ftplib
import datetime
# import win32com.client as win32
from apscheduler.schedulers.blocking import BlockingScheduler
# import pythoncom
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
		
	# 上传文件
	def upload_file(self,file_path,type,rename):
		url = "https://open.feishu.cn/open-apis/im/v1/files"
		headers = {"Authorization": "Bearer {}".format(self.access_token)}

	payload={'file_type': type,
			   'file_name': rename + "." + type }

		 # 准备上传文件的表单数据
        files = [
			('file': (rename + '.' + file_type, open(file_path, 'rb'), 'application/' + file_type))
        ]

	try:
            # 发送POST请求上传文件  
            response = requests.post(url, headers=headers, files=files)  
            response.raise_for_status()  # 如果请求失败，会抛出HTTPError异常  
              
            # 解析返回的数据  
            content = response.json()  
            if content.get("code") == 0:  
                # 成功上传文件，返回文件key等信息  
                return content["data"]["file_key"]  
            else:  
                # 上传失败，抛出异常或返回错误信息  
                raise Exception(f"文件上传失败，错误码：{content['code']}，错误信息：{content.get('msg')}")  
        except requests.exceptions.RequestException as e:  
            # 请求异常，抛出异常或返回错误信息  
            raise Exception(f"请求异常：{e}")  
        finally:  
            # 确保文件对象被关闭  
            if 'file' in files:  
                files['file'][1].close()
				

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


def login_down_file(remote_directory):
	# FTP服务器连接信息
	ftp_server = "10.0.74.192"
	ftp_user = "ftpcsxsjz"
	ftp_password = "yhcsx123456"

	# 打开FTP连接
	ftp = ftplib.FTP(ftp_server)
	ftp.encoding='utf-8'
	ftp.login(ftp_user, ftp_password)
	print(ftp.getwelcome())

	# 进入目标目录
	ftp.cwd(remote_directory)
	ftp.nlst()
	dir_name_1=ftp.nlst()[-1]+'/'
	ftp.cwd('./'+dir_name_1)
	dir_name_2=ftp.nlst()[-1]+'/通用结果/'
	ftp.cwd('./'+dir_name_2)
	ftp.nlst()
	dir_name_3=ftp.nlst()[-1]
	ftp.cwd('./'+dir_name_3)
	remote_file_name=ftp.nlst()[-1]
	print(remote_file_name)

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

def main():
	provinces = ['福建'] #改
	for province in provinces:
		login_down_file('WEB-INF/schedule/昨日采购订单稽核-{}/'.format(province))
	 # excel_catch_screen(province, vba_code)
		app_id="cli_a47c6dd3ec789013"
		app_secret="vMluLBgXzDQjBSPazRTSeeW8klHwNF2b"
		chat_name="彭承华"
		#chat_name="省区运营沟通群"
		messages_url="https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
		fei=FeishuApi(app_id,app_secret,chat_name,messages_url)
		res=fei.send_msg("以下是{}采购订单稽核：".format(province)) #改
	 #   res=fei.send_pic(os.path.join(os.path.dirname(os.path.abspath(__file__)),'115_csx_report_ksrb_bmkh.JPG'))  #改

if __name__ == '__main__':
	scheduler=BlockingScheduler()
	# scheduler.add_job(main,'cron',day_of_week='fri',hour='9',minute='42',coalesce=True, misfire_grace_time=300)
	scheduler.add_job(main,'cron',hour='15',minute='20',coalesce=True, misfire_grace_time=300)
	scheduler.start()
	#scheduler.remove_all_jobs(jobstore=None)
	#main()