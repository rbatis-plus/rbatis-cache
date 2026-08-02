//! rbatis 注册入口 — 不改动 rbatis 本体，通过公开字段/方法安装缓存。
//!
//! [`RbatisCacheExt`] 是 [`rbatis::RBatis`] 的扩展 trait：
//!
//! - [`RbatisCacheExt::install_cache`] 把拦截器插入 `RBatis::intercepts`
//!   最前（index 0，位于 Log/Page 之前；SQL 改写类拦截器请自行调整顺序），
//!   并把可选的事务监听器注册到 `RBatis::listeners`（上游
//!   `fix/transaction-listener` 分支提供的 hook）。

use std::sync::Arc;

use rbatis::intercept::Intercept;
use rbatis::plugin::transaction::TransactionListener;
use rbatis::RBatis;

/// 不修改 rbatis 本体的缓存安装扩展。
pub trait RbatisCacheExt {
    /// 安装缓存拦截器（可选附事务监听器）。
    ///
    /// 拦截器插入 `intercepts` 最前；监听器错误只记日志、不改变事务结果
    /// （上游契约保证）。
    fn install_cache(
        &self,
        intercept: Arc<dyn Intercept>,
        listener: Option<Arc<dyn TransactionListener>>,
    );
}

impl RbatisCacheExt for RBatis {
    fn install_cache(
        &self,
        intercept: Arc<dyn Intercept>,
        listener: Option<Arc<dyn TransactionListener>>,
    ) {
        self.intercepts.insert(0, intercept);
        if let Some(l) = listener {
            self.add_listener(l);
        }
    }
}
