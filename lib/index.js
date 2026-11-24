import { defineTool } from '@deepseek-ai/dsh-tools';
import z from '@deepseek-ai/schemastery';
/**
 * @dsh-external/dsh-stream-index-guard — 流式 index 容错插件。
 *
 * 病灶：上游模型/中继服务发出的流式数据不合规，reasoning-delta / text-delta /
 * tool-call-delta 块的 index 不是合法的非负安全整数（负数、小数、缺失、字符串），
 * DSH 的 AssistantStreamAccumulator.push / invariant.validateStream 直接抛
 * TypeError，整轮对话失败。
 *
 * 修法：通过 llm/stream waterfall 在 chunk 进入内置校验之前修正 index。
 *
 * 三条硬约束（踩过才知道）：
 *  1. waterfall 监听器必须**同步**返回 AsyncIterable —— cordis 是 `return next()`，
 *     不 await；写成 async 会返回 Promise，下游 `yield*` 立刻炸。
 *  2. index 不是"随便给个合法数字"就行：invariant.validateDelta 要求
 *     `open.get(index) === 期望块类型`。乱分配 +1 只会把 TypeError 换成
 *     "requires an open reasoning block"，照样整轮失败。所以必须修到**真实
 *     打开着的同类块**上，无法安放时丢弃该帧。
 *  3. 组装状态必须 per-stream。做成模块级全局会跨请求/跨会话互相污染。
 */
const name = "@dsh-external/dsh-stream-index-guard";
const inject = ['llm', 'tools'];
const Config = z.object({
  enabled: z.boolean().default(true).description('是否启用流式 index 自动修正'),
});
/** 累计统计（全局，跨流累计；只作观测，不参与修正决策）。 */
const stats = {
  'reasoning-delta': 0,
  'text-delta': 0,
  'tool-call-delta': 0,
  'block-start': 0,
  'block-end': 0,
  dropped: 0,
};
/** delta 类型 → 它必须落在哪种已打开的块上。 */
const BLOCK_OF_DELTA = {
  'reasoning-delta': 'reasoning',
  'text-delta': 'text',
  'tool-call-delta': 'tool-call',
};
const BLOCK_TYPES = new Set(['reasoning', 'text', 'tool-call']);
/** 一条流独立的组装状态：只有它知道哪些 index 真的打开过。 */
function createStreamState() {
  return {
    open: new Map(),
    lastOfType: { reasoning: -1, text: -1, 'tool-call': -1 },
    nextIndex: 0,
  };
}
function isGoodIndex(value) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && !Object.is(value, -0);
}
/** 取一个当前没被占用的最小 index。 */
function allocIndex(state) {
  while (state.open.has(state.nextIndex))
    state.nextIndex += 1;
  const index = state.nextIndex;
  state.nextIndex += 1;
  return index;
}
/**
 * 修正一个 chunk 的 index，返回新 chunk；返回 undefined 表示这条帧无法安放，丢弃。
 *
 * 永不就地改写传入对象：上游 chunk 可能已被 JSON 快照冻结，赋值会静默失败或抛错。
 */
function patchChunk(chunk, state) {
  if (!chunk || typeof chunk !== 'object')
    return chunk;
  switch (chunk.type) {
    case 'block-start': {
      let index = chunk.index;
      if (!isGoodIndex(index) || state.open.has(index)) {
        stats['block-start'] += 1;
        index = allocIndex(state);
        chunk = { ...chunk, index };
      }
      else {
        state.nextIndex = Math.max(state.nextIndex, index + 1);
      }
      const blockType = chunk.blockType;
      state.open.set(index, blockType);
      if (BLOCK_TYPES.has(blockType))
        state.lastOfType[blockType] = index;
      return chunk;
    }
    case 'block-end': {
      let index = chunk.index;
      if (!isGoodIndex(index) || !state.open.has(index)) {
        stats['block-end'] += 1;
        const want = chunk.block?.type;
        const fallback = BLOCK_TYPES.has(want) ? state.lastOfType[want] : -1;
        if (!isGoodIndex(fallback) || !state.open.has(fallback)) {
          stats.dropped += 1;
          return undefined;
        }
        index = fallback;
        chunk = { ...chunk, index };
      }
      state.open.delete(index);
      return chunk;
    }
    case 'reasoning-delta':
    case 'text-delta':
    case 'tool-call-delta': {
      const want = BLOCK_OF_DELTA[chunk.type];
      let index = chunk.index;
      if (!isGoodIndex(index) || state.open.get(index) !== want) {
        stats[chunk.type] += 1;
        const fallback = state.lastOfType[want];
        if (!isGoodIndex(fallback) || state.open.get(fallback) !== want) {
          stats.dropped += 1;
          return undefined;
        }
        index = fallback;
        chunk = { ...chunk, index };
      }
      return chunk;
    }
    default:
      return chunk;
  }
}
/**
 * 包装一条流：逐帧修正 index。同步返回 AsyncIterable（waterfall 契约）。
 */
function wrapStream(source) {
  if (source && typeof source.then === 'function') {
    // 外层还有 async 监听器时 next() 会给出 Promise；仍要同步返回 AsyncIterable。
    return (async function* () {
      const state = createStreamState();
      for await (const chunk of await source) {
        const patched = patchChunk(chunk, state);
        if (patched !== undefined)
          yield patched;
      }
    })();
  }
  const state = createStreamState();
  const iterator = source?.[Symbol.asyncIterator]?.();
  if (!iterator || typeof iterator.next !== 'function') {
    throw new TypeError('[stream-index-guard] llm/stream 的 next() 未返回 AsyncIterable');
  }
  return {
    async next() {
      for (;;) {
        const result = await iterator.next();
        if (result.done)
          return result;
        const patched = patchChunk(result.value, state);
        if (patched === undefined)
          continue;
        return { done: false, value: patched };
      }
    },
    async return() {
      return (await iterator.return?.()) ?? { done: true, value: undefined };
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}
function apply(ctx, config) {
  if (!config.enabled)
    return;
  // 同步返回 AsyncIterable。写成 async 会让 cordis 的 `return next()` 拿到 Promise。
  ctx.on('llm/stream', (options, next) => wrapStream(next()));
  ctx.effect(() => ctx.tools.register(defineTool({
    name: 'stream_index_guard_stats',
    description: '查看流式 index 容错插件拦下并修正了多少次非法 index',
    parameters: {},
    output: {
      schema: { type: 'string' },
      render: (_args, value) => [{ type: 'text', text: String(value) }],
    },
    async execute() {
      const fixed = Object.entries(stats)
        .filter(([key, count]) => key !== 'dropped' && count > 0)
        .map(([type, count]) => `  - ${type}: ${count} 次`);
      const total = fixed.length === 0 ? 0 : Object.entries(stats)
        .filter(([key]) => key !== 'dropped')
        .reduce((sum, [, count]) => sum + count, 0);
      if (total === 0 && stats.dropped === 0)
        return '✅ 至今没有遇到非法 index，流式数据全部合规。';
      const lines = [];
      if (total > 0)
        lines.push(`⚠️  累计修正 ${total} 次非法 index：`, ...fixed);
      if (stats.dropped > 0)
        lines.push(`🗑  丢弃 ${stats.dropped} 帧无法安放的坏数据（无同类已打开块可指向）。`);
      return lines.join('\n');
    },
  })), '@dsh-external/dsh-stream-index-guard: stats');
}
export { Config, name, inject, apply };
