# 钻探现场轮转日志·持续采集

面向合作资料接收人员的 Python 采集组件。导入已经关闭的完整日志，保留每条设备记录的启动代际与原始字节区间。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录，`GET /files` 返回每个文件身份的读取进度、坏行与数据缺口；按 Ctrl-C 停止。

## 持续读取

`follow` 常驻读取日志目录，设备端文件协议不变；`replay` 按事件脚本在临时目录演示写入、轮转、截断和重启；`status` 查看现场状态。

```bash
python3 -m collector replay --events fixtures/log-events.json
python3 -m collector follow --dir var/logs --db var/records.sqlite3 --interval 1
python3 -m collector status --db var/records.sqlite3
```

- 文件身份取 (设备, inode) 与文件头指纹，不看文件名：重命名轮转继续跟读，同名新文件从头读起，旧文件未读完就消失会登记数据缺口，不会悄悄跳到新文件结尾。
- 记录以 设备号:启动批次:序号 为标识，轮转重叠与崩溃重读不会重复入账；设备重启后的相同序号是新记录。
- 读取位置、未完成字节与确认进度随记录在同一事务提交，提交前后退出都能续跑，有效数据不重不漏。
- 坏行按字节范围和原因登记，后续好行不受影响；半行 UTF-8 字符等齐字节再解析。
- 按块流式读取并限制半行缓存上限，目录增长再快也不会把全部文件内容搬进内存。
- `status` 与 `/files` 可查到哪个设备停在半行、哪个轮转文件尚未读完。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。
`follow.py` 是持续读取器，按真实文件身份跟踪轮转日志；`simulator.py` 按事件脚本模拟设备端写入。
`fixtures/log-events.json` 描述本地写入、轮转、截断和重启顺序，供 `replay` 演示。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
