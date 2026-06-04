package com.stonewu.fusion.service.ai.agentscope;

import java.util.function.Supplier;

/**
 * Agent 执行取消令牌
 * <p>
 * 对取消检查逻辑的轻量封装，底层委托给外部提供的 {@link Supplier}（通常是 Redis 标志检查）。
 * 支持集群部署——无论 cancel 请求打到哪个节点，所有节点都能通过 Redis 感知到取消状态。
 * <p>
 * 使用位置：
 * - {@link AgentScopeEventBridge} — 每个 AgentEvent 触发时检查
 * - {@link AgentScopeToolAdapter#callAsync} — 每次工具执行前检查
 */
public class AgentCancellationToken {

    private final Supplier<Boolean> cancelledCheck;

    /**
     * @param cancelledCheck 取消状态检查函数，通常为 {@code () -> isCancelled(conversationId)}，
     *                       底层查询 Redis 标志
     */
    public AgentCancellationToken(Supplier<Boolean> cancelledCheck) {
        this.cancelledCheck = cancelledCheck;
    }

    /**
     * 是否已取消
     */
    public boolean isCancelled() {
        return Boolean.TRUE.equals(cancelledCheck.get());
    }

    /**
     * 检查是否已取消，如果已取消则抛出 {@link AgentCancelledException}
     *
     * @throws AgentCancelledException 如果已取消
     */
    public void throwIfCancelled() throws AgentCancelledException {
        if (isCancelled()) {
            throw new AgentCancelledException("Agent 执行已被用户取消");
        }
    }
}
