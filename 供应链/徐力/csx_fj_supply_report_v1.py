# 全国-供应链日报推送
# -*- coding: utf-8 -*-
# 读取excel内容转换为图片
from file_operator import FileUpload, FTPDownload
from PIL import ImageGrab, ImageDraw, Image, ImageFont
from apscheduler.schedulers.blocking import BlockingScheduler

import xlwings as xw
import pythoncom
import os
import time

SAVE_FILE_PATH = 'C:/soft/csx-py-report/cache_data/'
# SAVE_FILE_PATH = 'D:/workspace/csx-py-report/cache_data/'
REMOTE_PATH = 'WEB-INF/schedule/供应链日报-福建/'
# MSG_URL = 'http://10.192.1.27:8000/polaris/api/asset/battlereport/upload'
MSG_URL = 'http://api.b2bcsx.com/polaris/api/asset/battlereport/upload'
IMG_SUFFIX = 'PROFIT-'

def create_img(file_path, sheetname):

    img_name: str = time.strftime("%Y%m%d%H%M%S", time.localtime()) + '.png'
    os.system('taskkill /IM wps.exe /F')
    pythoncom.CoInitialize()
    # 使用xlwings的app启动
    app = xw.App(visible=False, add_book=False, )
    # 打开文件
    file_path = os.path.abspath(file_path)
    wb = app.books.open(file_path)
    # 选定sheet
    sheet = wb.sheets(sheetname)
    # 获取有内容的区域
    all = sheet.used_range
    # 复制图片区域
    all.api.CopyPicture()
    # 粘贴
    sheet.api.Paste()
    # 当前图片
    pic = sheet.pictures[-1]
    # 复制图片
    pic.api.Copy()
    time.sleep(3)  # 延迟一下操作，不然获取不到图片
    # 获取剪贴板的图片数据
    img = ImageGrab.grabclipboard()
    # 保存图片
    img.save(img_name)
    # 删除sheet上的图片
    pic.delete()
    # 不保存，直接关闭
    wb.close()
    # 退出xlwings的app启动
    app.quit()
    pythoncom.CoUninitialize()  # 关闭多线程
    return img_name


def change_color(img, water_maker):
    new_image_name: str = IMG_SUFFIX + time.strftime("%Y%m%d%H%M%S", time.localtime()) + '.png'
    result = change_img_bac(img, water_maker)
    result.save(SAVE_FILE_PATH + new_image_name)
    os.remove(img)

    return new_image_name

# 改变背景颜色
def change_img_bac(image_name, water_maker):
    image = Image.open(image_name)
    draw = ImageDraw.Draw(image)
    font = ImageFont.truetype("arial.ttf", 30)

    text_color = (0, 204, 153)  # 白色
    # 计算文字位置（左上角）
    text_position = (0, 0)

    # 在图片上绘制文字
    draw.text(text_position, water_maker, fill=text_color, font=font)

    if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
        alpha = image.convert('RGBA').split()[-1]
        background = Image.new("RGBA", image.size, (255, 255, 255, 255))
        background.paste(image, mask=alpha)
        return background
    else:
        return image


def operator(msg_title):
    # 时间戳水印
    date_marker = time.strftime("%Y-%m-%d %H:%M", time.localtime())
    # 接收消息的工号,分割多个
    #user_work_no = ('81005651,80948305,80000003,80894878,80000979,80000860,81040991,81167992,80081307,81129882,'
    #                '81214246,81129768,80080380,80160057,80013607,80766491,80965482,80894970,80005185,81226224,'
    #                '80000600,80006783,80890597,80913250,80917773,80724052,81036820,80092129,80080493,80948744,'
    #                '80953780,81023297,81096948,80164383,80885495,80852746,80935132')

    user_work_no = '80000975,80007823'

    origin_file = FTPDownload.login_down_file(REMOTE_PATH, SAVE_FILE_PATH)

    image_name = create_img(SAVE_FILE_PATH + origin_file, 'sheet1')

    new_image_name = change_color(image_name, date_marker)

    print('最终图片保存完成:' + new_image_name)

    FileUpload.send_pic_msg(MSG_URL, user_work_no, msg_title, SAVE_FILE_PATH + new_image_name)


if __name__ == '__main__':
    # pip install -r pack.txt
    # 帆软定时8：55分执行，推送可在每天09：15执行
    operator("供应链日报-福建")

