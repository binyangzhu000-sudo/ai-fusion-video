package com.stonewu.fusion.service.ai.agentscope;

import cn.hutool.json.JSONUtil;
import com.stonewu.fusion.service.ai.ToolExecutionContext;
import com.stonewu.fusion.service.ai.ToolExecutor;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.tool.ToolBase;
import io.agentscope.core.tool.ToolCallParam;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Adapts the project's {@link ToolExecutor} contract to AgentScope V2 {@link ToolBase}.
 */
@Slf4j
public class AgentScopeToolAdapter extends ToolBase {

    private final ToolExecutor toolExecutor;
    private final ToolExecutionContext toolContext;
    private final AgentCancellationToken cancellationToken;

    public AgentScopeToolAdapter(ToolExecutor toolExecutor, ToolExecutionContext toolContext,
            AgentCancellationToken cancellationToken) {
        super(ToolBase.builder()
                .name(toolExecutor.getToolName())
                .description(resolveDescription(toolExecutor))
                .inputSchema(resolveParameters(toolExecutor))
                .readOnly(toolExecutor.isReadOnly())
                .concurrencySafe(true));
        this.toolExecutor = toolExecutor;
        this.toolContext = toolContext;
        this.cancellationToken = cancellationToken;
    }

    private static String resolveDescription(ToolExecutor toolExecutor) {
        String desc = toolExecutor.getToolDescription();
        return desc != null ? desc : toolExecutor.getDisplayName();
    }

    private static Map<String, Object> resolveParameters(ToolExecutor toolExecutor) {
        String schema = toolExecutor.getParametersSchema();
        if (schema == null || schema.isBlank()) {
            return Map.of(
                    "type", "object",
                    "properties", Map.of());
        }
        try {
            return JSONUtil.parseObj(schema);
        } catch (Exception e) {
            log.warn("工具参数 Schema 解析失败: tool={}, schema={}",
                    toolExecutor.getToolName(), schema, e);
            return Map.of(
                    "type", "object",
                    "properties", Map.of());
        }
    }

    @Override
    public Mono<ToolResultBlock> callAsync(ToolCallParam param) {
        return Mono.fromCallable(() -> {
            // 工具执行前检查取消标志，避免已取消后仍执行耗时工具
            cancellationToken.throwIfCancelled();

            String input = JSONUtil.toJsonStr(param.getInput());
            log.info("[AgentScopeToolAdapter] 工具被调用: name={}, input={}", getName(), input);

            String result = toolExecutor.execute(input, toolContext);

            // 工具执行后再次检查，避免结果被提交回已取消的流
            cancellationToken.throwIfCancelled();

            return buildToolResult(param, result);
        }).onErrorResume(AgentCancelledException.class, e -> {
            log.info("[AgentScopeToolAdapter] 工具执行被取消: name={}", getName());
            return Mono.error(e);
        }).onErrorResume(e -> {
            if (e instanceof AgentCancelledException) {
                return Mono.error(e);
            }
            log.error("[AgentScopeToolAdapter] 工具执行失败: name={}", getName(), e);
            String errorResult = JSONUtil.toJsonStr(Map.of(
                    "status", "error",
                    "message", "工具执行失败: " + e.getMessage(),
                    "toolName", getName()));
            return Mono.just(buildToolResult(param, errorResult));
        });
    }

    private ToolResultBlock buildToolResult(ToolCallParam param, String result) {
        ToolResultBlock block = ToolResultBlock.text(result);
        ToolUseBlock toolUseBlock = param != null ? param.getToolUseBlock() : null;
        if (toolUseBlock == null) {
            return block;
        }
        return block.withIdAndName(toolUseBlock.getId(), toolUseBlock.getName());
    }
}
