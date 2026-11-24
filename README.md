# @dsh-external/dsh-stream-index-guard

把上游不合规的流式 `index` 修正到**真实打开着的块**上，让对话不再因为一条脏帧整轮失败。

## 治的是什么病

DSH 里这条报错：

```
本轮运行失败reasoning-delta index must be a non-negative safe integer
```

出处（`@deepseek-ai/dsh-llm`）：

- `types/assistant-stream.js` → `safeIndex()`，被 `AssistantStreamAccumulator.push` 调用
- `types/invariant.js` → `validateIndex()`，被 `validateStream` 调用

触发条件：上游模型或中继服务发出的 `reasoning-delta` / `text-delta` / `tool-call-delta` /
`block-start` / `block-end` 帧，`index` 不是合法的非负安全整数（负数、小数、字符串、缺失）。
默认逻辑直接抛 `TypeError`，整轮对话作废。

## 怎么修

通过 `llm/stream` waterfall 在 chunk 进入内置校验**之前**改写 index。

三条硬约束，缺一不可：

1. **监听器必须同步返回 `AsyncIterable`。**
   cordis 的 waterfall 是 `return next()`，不 `await`。写成 `async` 会返回
   `Promise<AsyncIterable>`，下游 `yield*` 立刻抛 "is not async iterable"。

2. **修到真实打开着的同类块上，而不是"随便给个合法数字"。**
   `invariant.validateDelta` 要求 `open.get(index) === 期望块类型`。
   简单 `+1` 或归零会把 `TypeError` 换成 `requires an open reasoning block`，
   照样整轮失败 —— 只是把 bug 换了个位置。
   无同类块可指向时，**丢弃该帧**（`stats.dropped`）。

3. **组装状态必须 per-stream。**
   做成模块级全局会跨请求、跨会话互相污染。

## 用法

装好即在。观测：

```
stream_index_guard_stats
```

```
⚠️  累计修正 3 次非法 index：
  - reasoning-delta: 3 次
🗑  丢弃 1 帧无法安放的坏数据（无同类已打开块可指向）。
```

零修正时返回「✅ 至今没有遇到非法 index，流式数据全部合规。」

## 构建 / 自检 / 装配

```bash
npm run verify     # 12 项自检，不需要宿主
npm run build      # 需要 DSH_CHECKOUT
```

正式装配（重启后仍生效）：

```
dev_install_package { "dir": "D:\\deepseek\\plugins\\dsh-stream-index-guard", "profile": "desktop" }
```

只注入当前运行时（重启失效）：

```
dev_inject_plugin { "dir": "D:\\deepseek\\plugins\\dsh-stream-index-guard" }
```

## 已知边界

- 只管 `index`，不修 `text` / `id` / `argumentsDelta` 的类型问题。
- 丢弃的帧会让对应块的文本少一段 —— 这比整轮失败好，但不是无损。
- 统计是进程内累计，热重载归零。
