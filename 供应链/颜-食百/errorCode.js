const errorCodeMap = {
  11100001: '行式引擎不支持多数据源展示',
  11100002: '行式引擎不支持条件属性修改行高列宽',
  11100003: '行式引擎不支持形态',
  11100004: '行式引擎只支持从上到下扩展属性，其它扩展属性设置均不支持',
  11100005: '行式引擎不支持悬浮元素',
  11100006: '行式引擎不支持层次坐标',
  11100007: '行式引擎不支持分栏',
  11100008: '行式引擎不支持Sheet间运算',
  11100009: '行式引擎不支持子报表',
  11100010: '导出 Excel 不支持该公式：公式xxx',
  11100011: '导出 Excel 不支持 HTML 中部分标签',
  11100012: '导出 Excel 单元格背景只支持纯色',
  11100013: '导出 Word 页眉页脚的内容为图片和文字组合时，文字不可编辑',
  11100014: '导出 Word 不支持多 sheet下的不同页面设置',
  11100023: '导出图片过大，请减少导出的内容',
  11100018: 'Flash 打印不支持将页面设置传递给打印机',
  11100021: 'PDF 打印只支持 IE 内核浏览器,其他浏览器将导出 PDF 文件',
  11100019: '未找到当前浏览器语言对应的国际化文件,将以中文作为默认语言',
  11100017: '压缩部署不支持远程设计',
  11100020: '当前 HSQL 数据库已被另一线程锁定',
  11100022: '您所访问的模板含有聚合报表，它并不支持填报预览',
  11100015: '系统并发数已满',
  11100016: '您使用了未注册的功能——xxxxx',
  11200001: '当前浏览器未安装打印所需插件',
  11200002: '当前工程目录下未发现 Applet包',
  11200003: '错误的公式参数 + 公式名',
  11200008: '合并单元格跨越冻结和非冻结单元格+具体单元格',
  11200004: '邮件配置参数不正确',
  11200005: '加密狗读取失败',
  11200006: '当前配置目录下未发现注册文件',
  11200007: '导入 Excel2007 需要在 lib 目录下放置poi包',
  11201000: 'JS 抛错',
  11201001: '无法赋值，模版未编辑单元格$cell',
  11202000: '图表读取失败',
  11300001: '数据集配置错误',
  11300021: '数据库连接失败',
  11300100: 'copy agent failed',
  11300101: 'JVM properties {} is not defined or is error value, please check it',
  11300002: '行高列宽超出页面大小设置：行/列 号',
  11300003: '单元格计算死循环 + 单元格行列',
  11300018: '单元格计算死循环或存在预期外的错误引用+单元格行列',
  11300009: '数据连接失败+数据库返回的日志',
  11300020: 'ODBC连接失败，找不到ODBC连接驱动',
  11300004: '找不到模板文件 + 模板名',
  11300005: '模板文件解析出错',
  11300015: '访问频率过高被拦截',
  11300016: '无法访问未授权的加密模板',
  11300006: '文件解析出错',
  11305000: '设备为授权，当前设备码：xxxxxxx',
  11300017: '报表预警中的各种异常提示信息',
  11300007: '登录失败',
  11300008: '会话超时',
  11300010: '找不到相关任务',
  11300011: '结果报表无权限访问',
  11300014: '找不到结果报表',
  11300200: '插入删除行失败',
  11300301: '日期格式有误，公式计算中止',
  11100015: '系统并发数已满',
  11100016: '您使用了未注册的功能——xxxxx',
  11200005: '加密狗读取失败',
  21300001: '服务器超时',
  21300002: '上传文件失败，文件过大',
  21300003: '上传文件失败，二进制头校验失败',
  21300004: '没有权限',
  21300005: '重复命名',
  21300037: '业务包行过滤器已经存在',
  21300006: '用户不可用',
  21300007: '用户名密码错误',
  21300008: '导入树数据集未清空原有数据',
  21300009: '导入加密方式改变未清空原有数据',
  21300010: '普通用户暂不支持此次操作',
  21300011: '新旧密码相同',
  21300025: '认证方式改变未踢出用户',
  21300026: '平台使用用户移除未踢出用户',
  21300028: '切换用户数据集未踢出用户',
  21300029: '旧密码错误',
  21300030: '你暂无权使用移动端，请联系管理员',
  21300050: '同步 id 冲突（名称对应多个 id ）',
  21300051: '同步名字冲突(一个 id 多个用户名)',
  21300052: '同步 id 与平台现有 id 冲突',
  21300053: '同步 id 为空',
  21300054: '树部门标记为数字时，勾选的的 id 与树标记不一致',
  21300055: '一个标记对应多个部门',
  21300056: '一个标记有多个父标记(一个部门存在多个父部门)',
  21300057: '部门找不到对应父部门',
  21300058: '一个部门存在多个部门标记',
  21300059: '部门树存在死循环',
  21300061: '角色名设置超出范围',
  21300027: '无法移动至目标位置',
  21300036: '目录节点不存在',
  21300014: '登录信息不可用',
  21300015: '特殊字符禁止使用',
  21300016: '访问记录不存在',
  21300018: '登录认证信息解析出错',
  21300019: '登录设备不匹配',
  21300020: '平台数据库未迁移',
  21300021: '状态服务器连接失败',
  21300022: '文件服务器连接失败',
  21300031: 'bash 配置失败',
  21300032: 'spider 集群服务启动失败',
  21300033: '操作系统不匹配',
  21300023: '备份文件不存在',
  21300062: '备份还原文件 读写失败',
  21300024: '设计器启动的平台，不支持升级  JAR 包',
  21300025: '更新升级异常',
  21300034: '该数据连接有用户正在编辑',
  21300035: '该数据连接已被删除',
  21300042: '该数据连接类型未适配',
  21300070: '数据内存预警',
  21300071: '数据集不存在',
  21300038: '上传的迁移文件内容为空',
  21300039: '导入 BI 模板失败',
  21300040: '导入节点类型与现存类型冲突',
  21300041: 'FTP 上传错误',
  21300044: '访问的定时调度结果文件不存在',
  21300045: '定时调度对象类型无法识别',
  21300043: '埋点收集触发限制',
  21300060: '设置水印密度值超出限制范围',
  21310096: '配置项key格式不正确',
  21310097: '配置项key为空',
  21310098: '配置项值校验失败',
  21310099: '找不到该配置项',
  22400001: '用户名不能为空',
  22400002: '没有该用户',
  22400009: '邮箱或手机不能为空',
  22400010: '手机号或者邮箱地址不存在',
  22400019: '该邮箱尚未绑定过账号',
  22400020: '该手机尚未绑定过账号',
  22400021: '无法邮箱验证且手机也不可以验证且登录者不是管理员',
  22400022: '无法验证邮箱但手机可以验证',
  22400023: '无法邮箱验证且手机也不可以验证且登录者是管理员',
  22400024: '无法验证手机且邮箱也可以验证且登录者不是管理员',
  22400025: '无法手机验证且邮箱也不可以验证且登录者是管理员',
  22400026: '无法验证手机但邮箱可以验证',
  22400011: '邮箱未与该账号绑定',
  22400012: '手机未与该账号绑定',
  22400017: '短信服务不可用',
  22400018: '邮件服务不可用',
  22400003: '新密码与原密码不能相同',
  22400005: '原密码不正确',
  22400006: '新密码不能为空',
  22400007: '请再次输入新密码',
  22400008: '两次输入的新密码不一致',
  22400013: '验证码超时',
  22400014: '验证码为空',
  22400016: '验证码不正确',
  22400004: '密码强度不符合规范',
  22400015: '首次登录需要修改初始密码',
  22400100: '验证码验证超过限制次数',
  22400101: '验证码发送太过频繁',
  22400102: '验证码发送超过当日限制',
  22400027: '账号已在其他平台登录（先登录优先，可以修改密码)',
  22400127: '账号已在其他平台登录（先登录优先，无法修改密码)',
  22400028: '账号已在其他平台登录（后登录优先）',
  22400027: '先登录优先',
  22400028: '后登录优先',
  22400029: '需要短信验证',
  22400030: '该用户已经存在',
  22400031: '该用户或者 IP 已经被锁定',
  22400032: '该用户的密码需要更新',
  22400033: '该用户的密码强度不满足强度',
  22400034: '不支持的设备类型',
  22400035: '本次登录需要滑块验证',
  22400036: '本次登录的密码错误，需要滑块验证',
  22400037: '数据连接对应驱动没找到',
  22400038: '此公式无法解析',
  22400040: '用户数大于用户属性lic的限制',
  22400041: '无法使用单点登录接口绕过安全校验',
  22400042: '无法重复请求，请稍后再试',
  22400043: '请求次数过多，请明天再试',
  22400044: 'lic 迁移验证码错误',
  22400045: 'lic 迁移验证码错误次数过多，请重新生成',
  22400046: 'lic 迁移验证码已失效，请重新生成',
  22400047: 'lic 迁移验工具过时，请重新申请工具',
  22400048: '请输入迁移验证码',
  22400049: '服务器未注册或者临时注册',
  31300012: '创建数据连接失败',
  31300013: '找不到数据连接',
  31300101: '远程设计无权限',
  31300102: '模板已锁定',
  31300103: '未找到合适的导出方法',
  31300104: '远程设计连接失败',
  31300105: '远程设计用户名密码错误',
  31300106: '远程设计无权限修改配置',
  11300104: '远程设计连接失败	',
  11300105: '远程设计用户名密码错误	',
  11300107: '远程设计功能未注册	',
  11300108: '证书域名信息错误	',
  11300109: '密码错误次数已达上限，已被锁定X分钟	',
  11300110: '当前密码不符合密码强度要求	',
  11300111: '当前密码已失效	',
  11300112: '远程设计无权限	',
};

const $messageContainer = $('#message-container');
const $exceptionContainer = $('#exception-container');
const $customMsg = $('#tip-detail');

const message = $.trim($messageContainer.text());
const exception = $exceptionContainer.html();
const defaultCode = 150510001;
const defaultMsg = '出错啦';

const [m, code = defaultCode, msg] = message.match(/错误代码.*?(\d+)(.*?$)/m) || [];
const params = new URL(location.href).searchParams;
const dataStr = decodeURIComponent(window.atob(params.get('data') || ''));
let userName = '';
let title = '';

try {
  const data = JSON.parse(dataStr);
  userName = data.userName;
  title = data.title;
} catch (error) {
  console.log("为传用户信息", error)
}
const codeMsg = errorCodeMap[code] || msg || message || defaultMsg;

const lastMsg = '【' + code + '】' + codeMsg;
const href = window.location.href;
let lastException = '\n<br/>';
lastException += '【页面名称】: ' + title + '\n<br/>';
lastException += '【来源】: <a href="' + href + '" target="_blank">' + href + '</a>\n<br/>';
lastException += '【data】: ' + dataStr + '\n<br/>';
lastException += '【异常标题】: ' + message + '\n<br/>';
lastException += '【异常栈】: <pre>' + exception + '</pre>\n';



$customMsg.html(lastMsg);

larkReport();
wechatReport();


function wechatReport() {
  const url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=32d2b8e2-92b5-432f-bf6d-7c89af9e634b';

  const reqData = {
      "exceptionCode": "150510000",
      "exceptionContent": lastException,
      "exceptionLevel": 0,
      "exceptionTitle": lastMsg,
      "exceptionType": 1,
      "operator": userName,
      "productName": "数据中心-帆软",
      "projectBeName": "",
      "projectFeName": "",
      "productId": 24,
      "bizScope": "数据中心-帆软",
  };
  console.log("🚀 ~ file: error.html ~ line 87 ~ reqData", reqData)

  const msgData = {
    "msgtype": "markdown_v2",
    "markdown_v2": {
        "content": `
            ${JSON.stringify(reqData, null, 2)}
          `,
        "mentioned_list":[],
        "mentioned_mobile_list":[]
    }
}

  fetch(url, {
      method: 'POST', // or 'PUT'
      body: JSON.stringify(msgData), 
      headers: {
          'Content-Type': 'application/json'
      }
  }).then(res => res.json())
  .catch(error => console.error('Error:', error))
  .then(response => console.log('Success:', response));
}
function larkReport() {
  const url = 'https://open.feishu.cn/open-apis/bot/v2/hook/8f155732-bc20-49f4-b3a3-6477c1562dcd';

  const reqData = {
      "exceptionCode": "150510000",
      "exceptionContent": lastException,
      "exceptionLevel": 0,
      "exceptionTitle": lastMsg,
      "exceptionType": 1,
      "operator": userName,
      "productName": "数据中心-帆软",
      "projectBeName": "",
      "projectFeName": "",
      "productId": 24,
      "bizScope": "数据中心-帆软",
  };
  console.log("🚀 ~ file: error.html ~ line 87 ~ reqData", reqData)

  const msgData = {
    "msg_type": "interactive",
    "card": {
      "config": {
        "wide_screen_mode": true
      },
      "header": {
        "title": {
          "tag": "plain_text",
          "content": reqData.exceptionTitle || '异常通知'
        },
        "template": 'red',
      },
      "elements": [
        {
          "tag": "markdown",
          "content": `
            ${JSON.stringify(reqData, null, 2)}
          `,
        },
      ]
    }
  }
  
  fetch(url, {
      method: 'POST', // or 'PUT'
      body: JSON.stringify(msgData), 
      headers: {
          'Content-Type': 'application/json'
      }
  }).then(res => res.json())
  .catch(error => console.error('Error:', error))
  .then(response => console.log('Success:', response));
}

function exceptionCenter() {
  const url = 'http://api.freshfood.cn/exception/exception/reportException';

  const reqData = {
      "exceptionCode": "150510000",
      "exceptionContent": lastException,
      "exceptionLevel": 0,
      "exceptionTitle": lastMsg,
      "exceptionType": 1,
      "operator": userName,
      "productName": "数据中心-帆软",
      "projectBeName": "",
      "projectFeName": "",
      "productId": 24,
      "bizScope": "数据中心-帆软",
  };
  console.log("🚀 ~ file: error.html ~ line 87 ~ reqData", reqData)
  
  fetch(url, {
      method: 'POST', // or 'PUT'
      body: JSON.stringify(reqData), 
      headers: {
          'Content-Type': 'application/json'
      }
  }).then(res => res.json())
  .catch(error => console.error('Error:', error))
  .then(response => console.log('Success:', response));
}