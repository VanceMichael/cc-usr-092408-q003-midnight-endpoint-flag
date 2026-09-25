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

## 跨日判定与计算版本

关闭窗口采用左闭右开区间：终点时刻不属于窗口，因此恰好止于本地午夜的窗口不跨日；带秒或微秒的端点、不同 UTC 偏移与夏令时切换都遵循同一语义。开放式关闭（`effective_until = null`）保持 `pending_confirmation`，不伪造结束时间。

影响计算规则以 `projection_version` 暴露在事件详情、机场汇总与受影响航班分页中，三者始终引用同一当前版本。修正已写入数据通过服务启动时的可审计迁移完成：更正后的行写入新版本，旧版本行保持不动，迁移运行与行级更正分别记录在 `projection_migrations` 与 `impact_corrections` 表中。三个查询接口都接受 `?projection_version=` 参数回看历史裁定版本。
