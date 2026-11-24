import type { Context } from '@deepseek-ai/cordis'
import type z from '@deepseek-ai/schemastery'

export declare const name: '@dsh-external/dsh-stream-index-guard'
export declare const inject: readonly ['llm', 'tools']

export interface Config {
  /** 是否启用流式 index 自动修正。 */
  enabled: boolean
}

export declare const Config: ReturnType<typeof z.object>

export declare function apply(ctx: Context, config: Config): void
