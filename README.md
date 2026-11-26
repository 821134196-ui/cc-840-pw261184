# CRD 存储版本迁移 · 本地模拟环境

在没有 Kubernetes 集群的情况下，完整模拟一个 CRD（`PointwiseJob`）从
`v1alpha1` 存储迁移到 `v1` 存储的全过程，重点演示三件真实集群里最难处理的事：

1. **字段保护**：两个版本各有独占字段，旧版有类型客户端更新对象时不能擦掉它们
2. **可回滚迁移**：迁移中途出错可以回到旧控制器，finalize 前回滚窗口始终开放
3. **并发正确性**：迁移器与业务客户端并发写同一对象时，靠 resourceVersion CAS 不丢更新

纯 Python 标准库 + 原生前端实现，无任何第三方依赖。

## 快速开始

```bash
# 启动迁移台（默认 127.0.0.1:8080）
python3 -I run.py
# 或指定端口
python3 -I run.py 0.0.0.0 9000

# 跑全部 22 个测试
python3 -I run_tests.py -v
```

打开浏览器访问 `http://127.0.0.1:8080`，点左栏 **▶ 播放完整迁移剧本** 即可看完整流程。

## 模拟的 CRD

API：`delivery.pointwise.io/v1alpha1` 与 `delivery.pointwise.io/v1`，Kind `PointwiseJob`。

| 字段类别 | v1alpha1 | v1 |
|---|---|---|
| 共享字段（同路径） | `spec.image` `spec.replicas` `spec.env` `status.phase` … | 同左 |
| 结构映射字段 | `spec.retries` | `spec.retryPolicy.maxRetries` |
| **v1alpha1 独占** | `spec.legacyTag` `spec.upstream.legacyEndpoint` `status.legacyAck` | — |
| **v1 独占** | — | `spec.timeoutSeconds` `spec.retryPolicy.backoffSeconds` `spec.nodeSelector` `status.scheduledNode` |

## 核心机制与代码对照

### 1. conversion webhook（`conversion.py`）

`Converter.convert_structural` 实现无状态结构转换，`serve_conversion_review`
模拟 webhook 的 `ConversionReview` 请求/响应协议（`/api/conversion-review`）。
纯结构转换天然会丢弃源版本独占字段——这正是需要兜底机制的原因。

### 2. convertedObject 注解兜底

真实集群在转换不是无损往返时，会把原始对象序列化进
`v1.crd.k8s.io/converted-object` 注解。本模拟器使用
`sim.crd.k8s.io/converted-object`，把非存储版本的独占字段以点路径为键收进去：

```json
"annotations": {
  "sim.crd.k8s.io/converted-object": "{\"spec.timeoutSeconds\": 42, ...}"
}
```

关键规则（`Converter._prune_stash`）：

- 只有**当前存储版本 schema 正式支持**的路径，stash 副本才算冗余可删
- 存储版本不认识的路径必须留在注解里，等待存储版本迁移后「转正」
- 客户端显式置 `null` 表示删除该字段

### 3. 读视图与写合并（字段保护的核心）

- **读** `read_view`：结构转换后，把 stash 中属于目标版本的独占字段叠回去
- **写** `write_merge`：客户端入参先转成存储版本骨架，再 **deep-merge 到现有
  存储对象**，而不是整对象替换。有类型客户端结构体里没有的字段因此不会被擦掉
- 有类型客户端（`simulator.TypedClient._trim_to_schema`）会保留 metadata
  annotations 原样带回——这与真实 Go 客户端行为一致，否则一次普通更新就会
  把兜底注解整体擦掉

### 4. resourceVersion 乐观锁（`store.py`）

- 全局单调 resourceVersion，写请求携带期望值做 CAS，不匹配返回 **409 Conflict**
- 客户端读-改-写循环遇 409 指数退避重试（`TypedClient.update`）
- `BlastRunner` 用 barrier + 模拟网络 RTT 窗口，确定性制造两个版本客户端的
  并发竞争，可在 UI 上直接看到一路成功、一路 409 重试

### 5. 迁移控制器（`migrator.py`，对标 kube-storage-version-migrator）

阶段机：

```
idle ──flip──▶ in_progress ──全部成功──▶ succeeded ──finalize──▶ finalized（不可逆）
                  │  │                       ▲
                  │  └──故障──▶ failed ──retry┘
                  └──────── rollback ◀────────┘  （succeeded/failed/in_progress 均可）
                        └──▶ rolling_back ──▶ rolled_back ──flip──▶ in_progress
```

- **flip**：CRD storage 版本翻转，新对象以新格式落盘，旧格式对象标记 pending
- **后台 worker**：逐对象「读最新值 → 转换 → CAS 重写」，对象级隔离，
  单对象失败不影响其他对象；CAS 冲突时退避后重读重转，绝不覆盖并发写入
- **故障注入**：按对象名或概率让转换失败。注入只在对象的首次尝试触发，
  模拟瞬时 webhook 抖动 —— 重试预算内自动恢复；把 `maxRetries` 设为 0 即可
  模拟持续故障，对象进入 `failed` 后需人工重试（与真实迁移器一致，避免错误风暴）
- **rollback**：finalize 之前随时可回滚，storage 切回旧版本，对象逐批写回，
  stash 中的旧版本字段重新「转正」
- **finalize 守卫**：存在未迁移对象、或任一 served 版本独占字段无法完整读回时
  拒绝下线旧版本；finalize 后旧版本返回 **410 Gone**，回滚窗口关闭

## 迁移台页面说明

- **左栏**：迁移流程按钮、故障注入/节奏配置、两个版本客户端的手动写入、一键剧本
- **中栏**：每个对象三个 tab —— 磁盘存储格式（真实落盘内容）、v1alpha1
  客户端视图、v1 客户端视图；颜色高亮独占字段、stash 注解和本次写入触及路径
- **右栏**：SSE 实时事件流（冲突、重试、重写、失败、回滚分色显示）
- 每个对象卡片上的 **⚡ 并发夹击** 按钮可随时夹入并发更新

## HTTP API 速览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/overview` | CRD + 迁移器 + 全部对象三视图 |
| GET | `/api/objects/{name}` | 单对象磁盘格式与两个客户端视图 |
| POST | `/api/objects` | 按任意 served 版本创建对象 |
| POST | `/api/objects/{name}/patch` | 指定版本客户端做 spec 局部更新 |
| PUT | `/api/objects/{name}` | 整体更新（带 resourceVersion 做 CAS） |
| POST | `/api/objects/{name}/blast` | 两个版本并发夹击 |
| POST | `/api/conversion-review` | 模拟 conversion webhook 协议 |
| POST | `/api/migrate/flip` | 翻转 storage 版本 |
| POST | `/api/migrate/start` `/retry` `/rollback` `/finalize` | 迁移控制 |
| POST | `/api/migrator/config` | 故障注入与节奏配置 |
| GET | `/api/events` | SSE 事件流 |

## 目录结构

```
crd_migrator/
├── crd.py          # CRD 定义：版本标志、字段归属、结构映射
├── conversion.py   # 转换 webhook、stash 注解、读写合并
├── store.py        # 模拟 etcd：resourceVersion、CAS、迁移状态
├── migrator.py     # 迁移控制器：阶段机、worker、故障注入、回滚、finalize
├── simulator.py    # 有类型客户端、读-改-写重试、并发夹击
├── events.py       # 事件总线（SSE 后端）
├── apiserver.py    # HTTP API + SSE + 静态页
├── app.py          # 装配与重置
├── seed.py         # 三个种子对象
└── static/         # 迁移台单页（index.html + app.js）
tests/              # 22 个测试：转换/字段保护/CAS/迁移/回滚/finalize/HTTP
run.py              # 启动入口
run_tests.py        # 测试入口
```

## 与真实 Kubernetes 的差异

这是教学模拟器，刻意简化了：webhook 为进程内调用而非真实 HTTPS 服务；
存储为内存 dict 而非 etcd；没有 watch 缓存/informer 分片；stash 注解用点路径
扁平存储而非完整对象快照。机制语义（CAS、转换兜底、合并写入、迁移阶段、
回滚窗口、finalize 守卫）与真实行为保持一致。
