# 机场中断影响服务

本项目提供纯后端机场中断影响服务，接收机场关闭、延长关闭和恢复开放事件，计算受影响航班与旅客，并把事件链和计算结果保存到 SQLite。示例机场、航班和旅客数量均为合成数据，运行时不请求外部服务。

## 测试命令

```bash
python3 -m unittest discover -s tests -v
```

## 编译与构建命令

```bash
python3 -m compileall -q app
```

## 启动命令

```bash
DB_PATH=./data/disruptions.db PORT=8080 python3 -m app
```

接口提供事件提交、单事件查询、机场影响汇总、受影响航班分页查询和健康检查。输入时间必须携带时区，事件标识具备幂等语义，事件链版本递增，SQLite 文件路径可通过 `DB_PATH` 调整。
