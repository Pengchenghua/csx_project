import requests  
import json  
import datetime  
  
# 定义FeishuApi类，用于与飞书API交互  
class FeishuApi:  
    def __init__(self, app_id, app_secret, chat_name, messages_url):  
        self.app_id = app_id  
        self.app_secret = app_secret  
        self.chat_name = chat_name  
        self.messages_url = messages_url  
          
        # 获取访问令牌  
        self.access_token = self.get_access_token()  
          
        # 设置请求头部  
        self.headers = {  
            "Authorization": "Bearer {}".format(self.access_token),  
            "Content-Type": "application/json;charset=utf-8"  
        }  
      
    # 获取访问令牌  
    def get_access_token(self):  
        try:  
            response = requests.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/", json={  
                "app_id": self.app_id,  
                "app_secret": self.app_secret  
            })  
              
            if response.status_code == 200:  
                data = response.json()  
                return data.get("tenant_access_token")  
            else:  
                raise Exception(f"Failed to get access token. Status code: {response.status_code}")  
        except Exception as e:  
            return {"error": str(e)}  
      
    # 获取群列表，并查找指定名称的群  
    def get_chat_list(self):  
        try:  
            response = requests.get("https://open.feishu.cn/open-apis/chat/v4/list", headers=self.headers)  
              
            if response.status_code == 200:  
                data = response.json()  
                groups = data.get("data", {}).get("groups")  
                  
                for group in groups:  
                    if group.get("name") == self.chat_name:  
                        return group  
                  
                raise Exception("Chat with the specified name not found.")  
            else:  
                raise Exception(f"Failed to get chat list. Status code: {response.status_code}")  
        except Exception as e:  
            return {"error": str(e)}  
  
# 示例用法（需要传入相应的参数）  
if __name__ == "__main__":  
    app_id = "cli_a47c6dd3ec789013"  
    app_secret = "vMluLBgXzDQjBSPazRTSeeW8klHwNF2b"  
    chat_name = "彭承华"  
    messages_url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"  
      
    feishu_api = FeishuApi(app_id, app_secret, chat_name, messages_url)  
    chat_info = feishu_api.get_chat_list()  
      
    if isinstance(chat_info, dict) and "error" in chat_info:  
        print("Error:", chat_info["error"])  
    else:  
        print("Chat info:", chat_info)