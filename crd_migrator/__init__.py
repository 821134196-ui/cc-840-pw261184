"""本地模拟的 CRD 存储版本迁移环境。

模块划分：
- crd:        CRD 定义（版本、served/storage 标志、字段归属）
- conversion: 模拟 conversion webhook、convertedObject 注解兜底、读写合并
- store:      模拟 etcd（resourceVersion 乐观锁、CAS）
- migrator:   模拟 storage-version-migrator 控制器（后台重写、失败注入、回滚）
- simulator:  有类型的旧/新客户端 + 并发更新驱动器
- apiserver:  HTTP API + SSE 事件流 + 静态页
- app:        装配入口
"""

__all__ = ["crd", "conversion", "store", "migrator", "simulator", "apiserver", "app"]
__version__ = "1.0.0"
