/**
 * 自检：dsh-stream-index-guard。
 *
 * 不依赖宿主 —— 用 mock ctx 抓 apply() 注册的 llm/stream 监听器，
 * 然后直接喂构造好的流，断言 index 修正行为。
 *
 * 跑法：node scripts/verify.mjs
 *
 * 断言的是**能过 invariant 校验**这件事，而不只是"数字合法"：
 * DSH 的 invariant.validateDelta 要求 open.get(index) === 期望块类型，
 * 所以每条 delta 修正后的 index 必须指向一个真的打开着的同类块。
 */
import { apply } from '../lib/index.js'

let failed = 0
const results = []

function check(name, ok, detail) {
  results.push({ name, ok, detail })
  if (!ok) failed += 1
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? `  — ${detail}` : ''}`)
}

/** 抓取 apply 注册的东西。 */
function mockCtx() {
  const hooks = new Map()
  const tools = new Map()
  const ctx = {
    on(name, cb) {
      if (!hooks.has(name)) hooks.set(name, [])
      hooks.get(name).push(cb)
      return () => {}
    },
    effect(fn) {
      return fn()
    },
    tools: {
      register(def) {
        tools.set(def.name, def)
        return () => {}
      },
    },
  }
  return { ctx, hooks, tools }
}

function streamOf(chunks) {
  return {
    [Symbol.asyncIterator]() {
      let i = 0
      return {
        async next() {
          if (i >= chunks.length) return { done: true, value: undefined }
          return { done: false, value: structuredClone(chunks[i++]) }
        },
        async return() {
          return { done: true, value: undefined }
        },
      }
    },
  }
}

/** 用真实注册的监听器跑一条流。保留完整 chunk（校验需要 blockType/block）。 */
async function run(hooks, chunks) {
  const listener = hooks.get('llm/stream')[0]
  const result = listener({}, () => streamOf(chunks))
  const out = []
  for await (const c of result) out.push(c)
  return { result, out }
}

/** 按 DSH invariant.validateStream 的规则校验一串 chunk 是否合法。 */
function validateLikeDsh(chunks) {
  const open = new Map()
  for (const c of chunks) {
    const idx = c.index
    if (c.type === 'finish') continue
    if (idx !== undefined && (!Number.isSafeInteger(idx) || idx < 0)) {
      return `非法 index: ${String(idx)} (${c.type})`
    }
    if (c.type === 'block-start') {
      if (open.has(idx)) return `重复 block-start: ${idx}`
      open.set(idx, c.blockType)
    } else if (c.type === 'reasoning-delta' || c.type === 'text-delta' || c.type === 'tool-call-delta') {
      const want = c.type.replace(/-delta$/, '')
      const actual = open.get(idx)
      if (actual !== want) return `${c.type} @${idx} 需要打开的 ${want} 块，实际 ${String(actual)}`
    }
  }
  return null
}

const { ctx, hooks, tools } = mockCtx()
apply(ctx, { enabled: true })

check('注册了 llm/stream 监听器', (hooks.get('llm/stream') ?? []).length === 1)
check('注册了 stats 工具', tools.has('stream_index_guard_stats'))

// A. 完全合规的流：必须原样通过
{
  const { result, out } = await run(hooks, [
    { type: 'block-start', index: 0, blockType: 'reasoning' },
    { type: 'reasoning-delta', index: 0, text: 'ok' },
    { type: 'block-end', index: 0, block: { type: 'reasoning', text: 'ok' } },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  check('A 合规流：同步返回（非 Promise）', typeof result.then !== 'function')
  check('A 合规流：零改动', JSON.stringify(out) === JSON.stringify([
    { type: 'block-start', index: 0, blockType: 'reasoning' },
    { type: 'reasoning-delta', index: 0, text: 'ok' },
    { type: 'block-end', index: 0, block: { type: 'reasoning', text: 'ok' } },
    { type: 'finish', reason: { kind: 'stop' } },
  ]), JSON.stringify(out))
  check('A 合规流：能过 DSH 校验', validateLikeDsh(out) === null, validateLikeDsh(out) ?? '')
}

// B. index 全炸但存在可指向的 open 块：修正后必须能过校验
{
  const { out } = await run(hooks, [
    { type: 'block-start', index: 0, blockType: 'reasoning' },
    { type: 'reasoning-delta', index: -1, text: 'a' },
    { type: 'reasoning-delta', index: 1.5, text: 'b' },
    { type: 'reasoning-delta', index: 'x', text: 'c' },
    { type: 'block-end', index: 0, block: { type: 'reasoning', text: 'abc' } },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  check('B 非法 index：全部修到 open 块', out.filter(c => c.type === 'reasoning-delta').every(c => c.index === 0),
    JSON.stringify(out.map(c => c.index)))
  check('B 非法 index：能过 DSH 校验', validateLikeDsh(out) === null, validateLikeDsh(out) ?? '')
}

// C. 没有任何已打开块就来了 delta：必须丢弃，不能硬塞
{
  const { out } = await run(hooks, [
    { type: 'reasoning-delta', index: -1, text: 'orphan' },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  check('C 无块可指：丢弃孤儿 delta', out.length === 1 && out[0].type === 'finish', JSON.stringify(out))
}

// D. 类型错配：reasoning-delta 指向 text 块 —— 必须改到同类块
{
  const { out } = await run(hooks, [
    { type: 'block-start', index: 0, blockType: 'text' },
    { type: 'block-start', index: 1, blockType: 'reasoning' },
    { type: 'reasoning-delta', index: 0, text: 'goes-here' },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  check('D 类型错配：改到真实 reasoning 块', out.some(c => c.type === 'reasoning-delta' && c.index === 1),
    JSON.stringify(out))
  check('D 类型错配：能过 DSH 校验', validateLikeDsh(out) === null, validateLikeDsh(out) ?? '')
}

// E. 状态隔离：两次独立流互不污染
{
  const first = await run(hooks, [
    { type: 'block-start', index: 5, blockType: 'reasoning' },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  const second = await run(hooks, [
    { type: 'block-start', index: -1, blockType: 'reasoning' },
    { type: 'finish', reason: { kind: 'stop' } },
  ])
  check('E 状态隔离：新流的非法 index 从 0 起', second.out[0].index === 0,
    `first=${second.out[0].index} prev=${first.out[0].index}`)
}

// F. 工具可读
{
  const text = await tools.get('stream_index_guard_stats').execute({})
  check('F stats 工具可执行', typeof text === 'string' && text.length > 0, text.split('\n')[0])
}

const passed = results.length - failed
console.log(`\n${failed === 0 ? '✅' : '❌'} ${passed}/${results.length} 通过`)
process.exit(failed === 0 ? 0 : 1)
