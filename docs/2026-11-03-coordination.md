# 流匹配与设备层联动优化

本地候选源码增加不可变 `Pattern` 的 `IsZero`、`ValidateText` 与字节级 `Line`。`Line` 保留旧提示符正则的匹配偏移和消费边界，支持不能嵌入 UTF-8 正则的原始 GB 字节；空或包含 CR/LF 的字面量在预检时拒绝。调用者分别保存原始提示符与解码文本，不将文本反向作为线缆字节。

超过 256 字节的字面量使用共享的不可变前缀表；进度、已发现位置均在单次等待内维护。纯字面量 All 保留先前命中并在消费后重置。私有过滤器的已证明安全分支借用输入，由同步输出边界消费；公开 Redactor 仍返回独立持有的字节。两套脱敏策略只共享前缀表计算，不合并规则更新和替换语义。

Send 和 SendAll 共用写入接纳、工作线程、计数校验和成功/失败收尾。普通阻塞流仍保留可取消的后台写入，SendAll 仍主动排空有界接收队列；没有增加每会话常驻写线程。单次写、完整写和立即写的原语义保持不变。

`WithTranscript` 重复入口已删除，调用者使用 `WithOutput`。候选验证与发布依赖验收不同；本轮未提交或发布版本。

验证与基准结果由本轮实际执行后附录记录，不以历史结果代替候选证明。

本地 Go 1.27.1 Darwin arm64 验证：完整 `script/ci` 通过，包含 race/shuffle、vet、七个各 100000 次 fuzz、基准 smoke、示例及独立消费者；另一次完整 race 测试通过。Go 1.26/Linux 不由这些结果代替。

十组串行交错样本使用同一 Apple M5、GOMAXPROCS=4、100ms/样本。长 4097 字节重复前缀、16KiB 输入、32 字节分片的内部游标从中位 3.066ms/2.50MB/513 allocations 降至 25.53µs/0B/0 allocations。私有安全 16KiB 块从 1103ns/16384B/1 allocation 降至 164.7ns/0B/0 allocations。短接收对照为 21.13µs→21.40µs（45 allocations 不变），公开 Redactor 对照为 162.7ns→164.7ns（1048B/4 allocations 不变），均未达到 5% 退化线。这些局部工作负载不代表全链路或网络设备速度。原始样本与门禁日志见 `benchmarks/2026-11-03/`。

补充：Go 1.26.0 Darwin arm64 独立 `go test -race ./...` 通过；两库同版完整配对 race/vet 通过。Go 1.27.1 Linux amd64 候选配对交叉编译通过；本机 Docker daemon 不可用，未运行 Linux 测试。配对日志归档在 connector 的 `docs/validation/2026-11-03/`。
