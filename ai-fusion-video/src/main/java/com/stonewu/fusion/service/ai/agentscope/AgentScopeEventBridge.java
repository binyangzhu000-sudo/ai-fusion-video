package com.stonewu.fusion.service.ai.agentscope;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockEndEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultDataDeltaEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.ToolResultState;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges AgentScope 2.x AgentEvent streams into the application's SSE payloads.
 */
@Slf4j
public class AgentScopeEventBridge {

    private static final String MAIN_SCOPE_KEY = "__main__";

    private final Sinks.Many<AiChatStreamRespVO> eventSink;
    private final String conversationId;
    private final String messageId;
    private final String mainAgentName;
    private final AgentCancellationToken cancellationToken;

    private final ConcurrentHashMap<String, EventScope> scopes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Agent> activeAgents = new ConcurrentHashMap<>();

    public AgentScopeEventBridge(Sinks.Many<AiChatStreamRespVO> eventSink,
            String conversationId,
            String messageId,
            String mainAgentName,
            AgentCancellationToken cancellationToken) {
        this.eventSink = eventSink;
        this.conversationId = conversationId;
        this.messageId = messageId;
        this.mainAgentName = mainAgentName;
        this.cancellationToken = cancellationToken;
    }

    public void handleMainEvent(AgentEvent event) {
        handleEvent(event, mainAgentName, null);
    }

    public void handleSubAgentEvent(AgentEvent event, String agentName, String parentToolCallId) {
        handleEvent(event, agentName, parentToolCallId);
    }

    private void handleEvent(AgentEvent event, String agentName, String parentToolCallId) {
        cancellationToken.throwIfCancelled();
        if (event == null) {
            return;
        }

        EventScope scope = scopes.computeIfAbsent(scopeKey(parentToolCallId), key -> new EventScope());

        if (event instanceof ThinkingBlockDeltaEvent e) {
            handleThinkingDelta(scope, agentName, parentToolCallId, e.getDelta());
        } else if (event instanceof ThinkingBlockEndEvent) {
            emitReasoningDurationIfNeeded(scope, agentName, parentToolCallId);
        } else if (event instanceof TextBlockDeltaEvent e) {
            handleTextDelta(scope, agentName, parentToolCallId, e.getDelta());
        } else if (event instanceof ToolCallStartEvent e) {
            scope.toolCall(e.getToolCallId()).name = e.getToolCallName();
        } else if (event instanceof ToolCallDeltaEvent e) {
            scope.toolCall(e.getToolCallId()).arguments.append(StrUtil.nullToEmpty(e.getDelta()));
        } else if (event instanceof ToolCallEndEvent e) {
            emitToolCall(scope, agentName, parentToolCallId, e.getToolCallId());
        } else if (event instanceof ToolResultStartEvent e) {
            scope.toolCall(e.getToolCallId()).name = e.getToolCallName();
        } else if (event instanceof ToolResultTextDeltaEvent e) {
            scope.toolCall(e.getToolCallId()).result.append(StrUtil.nullToEmpty(e.getDelta()));
        } else if (event instanceof ToolResultDataDeltaEvent e) {
            scope.toolCall(e.getToolCallId()).result.append(JSONUtil.toJsonStr(e.getData()));
        } else if (event instanceof ToolResultEndEvent e) {
            emitToolFinished(scope, agentName, parentToolCallId, e.getToolCallId(), e.getState());
        } else if (event instanceof AgentEndEvent && isSubAgent(agentName)) {
            emitEvent(new AiChatStreamRespVO()
                    .setMessageId(messageId)
                    .setConversationId(conversationId)
                    .setOutputType("SUB_AGENT_FINISHED")
                    .setParentToolCallId(parentToolCallId)
                    .setAgentName(agentName)
                    .setFinished(false));
        }
    }

    public void registerActiveAgent(Agent agent) {
        if (agent == null) {
            return;
        }
        activeAgents.put(getAgentKey(agent), agent);
    }

    public void unregisterActiveAgent(Agent agent) {
        if (agent == null) {
            return;
        }
        activeAgents.remove(getAgentKey(agent));
    }

    public void interruptTrackedAgents() {
        activeAgents.forEach((agentKey, agent) -> {
            try {
                agent.interrupt();
                log.info("[AgentScopeEventBridge] 已发送 interrupt 信号: agentKey={}, agentName={}",
                        agentKey, agent.getName());
            } catch (Exception e) {
                log.warn("[AgentScopeEventBridge] 发送 interrupt 信号失败: agentKey={}, agentName={}",
                        agentKey, agent.getName(), e);
            }
        });
    }

    public void clearTrackedAgents() {
        activeAgents.clear();
        scopes.clear();
    }

    private void handleThinkingDelta(EventScope scope, String agentName,
            String parentToolCallId, String delta) {
        if (StrUtil.isEmpty(delta)) {
            return;
        }
        scope.reasoningStartTime = scope.reasoningStartTime > 0
                ? scope.reasoningStartTime
                : System.currentTimeMillis();

        emitEvent(new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("REASONING")
                .setReasoningContent(delta)
                .setReasoningStartTime(scope.reasoningStartTime)
                .setParentToolCallId(parentToolCallId)
                .setAgentName(isSubAgent(agentName) ? agentName : null)
                .setFinished(false));
    }

    private void handleTextDelta(EventScope scope, String agentName,
            String parentToolCallId, String delta) {
        if (StrUtil.isEmpty(delta)) {
            return;
        }

        Long durationMs = null;
        if (scope.reasoningStartTime > 0 && scope.reasoningDurationMs == null) {
            durationMs = System.currentTimeMillis() - scope.reasoningStartTime;
            scope.reasoningDurationMs = durationMs;
        }

        AiChatStreamRespVO resp = new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("CONTENT")
                .setContent(delta)
                .setParentToolCallId(parentToolCallId)
                .setAgentName(isSubAgent(agentName) ? agentName : null)
                .setFinished(false);
        if (durationMs != null) {
            resp.setReasoningDurationMs(durationMs);
        }
        emitEvent(resp);
    }

    private void emitReasoningDurationIfNeeded(EventScope scope, String agentName, String parentToolCallId) {
        if (scope.reasoningStartTime <= 0 || scope.reasoningDurationMs != null) {
            return;
        }

        Long durationMs = System.currentTimeMillis() - scope.reasoningStartTime;
        scope.reasoningDurationMs = durationMs;
        emitEvent(new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("CONTENT")
                .setReasoningDurationMs(durationMs)
                .setParentToolCallId(parentToolCallId)
                .setAgentName(isSubAgent(agentName) ? agentName : null)
                .setFinished(false));
    }

    private void emitToolCall(EventScope scope, String agentName, String parentToolCallId, String toolCallId) {
        ToolCallAccumulator toolCall = scope.toolCall(toolCallId);
        String toolName = StrUtil.blankToDefault(toolCall.name, "unknown_tool");
        String arguments = StrUtil.blankToDefault(toolCall.arguments.toString(), "{}");

        log.info("[AgentScopeEventBridge] 工具调用开始: agent={}, tool={}, callId={}",
                agentName, toolName, toolCallId);

        emitEvent(new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("TOOL_CALL")
                .setToolCalls(List.of(new AiChatStreamRespVO.ToolCallVO()
                        .setId(toolCallId)
                        .setName(toolName)
                        .setArguments(arguments)))
                .setParentToolCallId(parentToolCallId)
                .setAgentName(isSubAgent(agentName) ? agentName : null)
                .setFinished(false));
    }

    private void emitToolFinished(EventScope scope, String agentName, String parentToolCallId,
            String toolCallId, ToolResultState state) {
        ToolCallAccumulator toolCall = scope.toolCall(toolCallId);
        String toolName = StrUtil.blankToDefault(toolCall.name, "unknown_tool");
        String resultText = stripSessionId(toolCall.result.toString());
        String toolStatus = detectToolStatus(resultText, state);

        log.info("[AgentScopeEventBridge] 工具调用完成: agent={}, tool={}, status={}",
                agentName, toolName, toolStatus);

        emitEvent(new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("TOOL_FINISHED")
                .setToolCallId(toolCallId)
                .setToolName(toolName)
                .setToolResult(resultText)
                .setToolStatus(toolStatus)
                .setParentToolCallId(parentToolCallId)
                .setAgentName(isSubAgent(agentName) ? agentName : null)
                .setFinished(false));
    }

    private synchronized void emitEvent(AiChatStreamRespVO event) {
        Sinks.EmitResult result = eventSink.tryEmitNext(event);
        if (result.isFailure()) {
            log.warn("[AgentScopeEventBridge] 事件发送失败: result={}, type={}, agentName={}, content={}",
                    result, event.getOutputType(), event.getAgentName(),
                    event.getContent() != null
                            ? event.getContent().substring(0, Math.min(event.getContent().length(), 100))
                            : null);
        }
    }

    private String detectToolStatus(String toolResult, ToolResultState state) {
        if (state == ToolResultState.ERROR
                || state == ToolResultState.INTERRUPTED
                || state == ToolResultState.DENIED) {
            return "error";
        }
        if (toolResult == null || toolResult.isBlank()) {
            return "success";
        }
        try {
            if (toolResult.trim().startsWith("{")) {
                JSONObject json = JSONUtil.parseObj(toolResult);
                String status = json.getStr("status");
                if ("error".equals(status) || "not_implemented".equals(status)) {
                    return "error";
                }
            }
        } catch (Exception ignored) {
        }
        String lower = toolResult.toLowerCase();
        if (lower.contains("工具执行失败") || lower.contains("执行异常")) {
            return "error";
        }
        return "success";
    }

    private String stripSessionId(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        return text.replaceAll("(?m)^session_id:.*\\n*", "").trim();
    }

    private String scopeKey(String parentToolCallId) {
        return StrUtil.blankToDefault(parentToolCallId, MAIN_SCOPE_KEY);
    }

    private boolean isSubAgent(String agentName) {
        return !mainAgentName.equals(agentName);
    }

    private String getAgentKey(Agent agent) {
        return agent.getName() + ":" + System.identityHashCode(agent);
    }

    private static class EventScope {
        private volatile long reasoningStartTime;
        private volatile Long reasoningDurationMs;
        private final ConcurrentHashMap<String, ToolCallAccumulator> toolCalls = new ConcurrentHashMap<>();

        ToolCallAccumulator toolCall(String toolCallId) {
            return toolCalls.computeIfAbsent(StrUtil.blankToDefault(toolCallId, "unknown"), id -> new ToolCallAccumulator());
        }
    }

    private static class ToolCallAccumulator {
        private volatile String name;
        private final StringBuilder arguments = new StringBuilder();
        private final StringBuilder result = new StringBuilder();
    }
}
