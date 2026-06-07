package com.stonewu.fusion.service.ai.agentscope;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.Event;
import io.agentscope.core.agent.EventSource;
import io.agentscope.core.agent.EventType;
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
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ThinkingBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges AgentScope events into the application's SSE payloads.
 * <p>
 * AgentScope 2.0-RC1 的 {@code streamEvents()} 暂时不会转发子 Agent 来源事件，
 * 所以主调用临时走旧 {@code stream()} / {@code Flux<Event>} 路径。这里同时保留
 * 新 {@link AgentEvent} 和旧 {@link Event} 两套转换入口，方便未来 AgentScope 完整
 * 支持子来源 {@code streamEvents()} 后删除旧 Event 兼容层。
 */
@Slf4j
public class AgentScopeEventBridge {

    private static final String MAIN_SCOPE_KEY = "__main__";
    private static final String SUBAGENT_EVENT_METADATA = "subagent_event";
    private static final String SUBAGENT_NAME_METADATA = "subagent_name";
    private static final String SUBAGENT_ID_METADATA = "subagent_id";
    private static final String SUBAGENT_SESSION_ID_METADATA = "subagent_session_id";

    private final Sinks.Many<AiChatStreamRespVO> eventSink;
    private final String conversationId;
    private final String messageId;
    private final String mainAgentName;
    private final AgentCancellationToken cancellationToken;

    private final ConcurrentHashMap<String, EventScope> scopes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Agent> activeAgents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> legacyAgentKeyToParentToolCallId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> legacyAgentIdToParentToolCallId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> legacySessionIdToParentToolCallId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> legacySyntheticParentToolCalls = new ConcurrentHashMap<>();
    private final Set<String> legacyActiveRootToolCallIds = ConcurrentHashMap.newKeySet();
    private final Set<String> legacyParentToolCallIdsWithChildEvents = ConcurrentHashMap.newKeySet();
    /** 记录存在并行冲突的 agentId（同一 agentId 对应多个不同 toolCallId） */
    private final Set<String> legacyAgentIdCollisions = ConcurrentHashMap.newKeySet();
    private volatile String latestLegacyRootToolCallId;

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

    public void handleMainEvent(Event event) {
        handleLegacyEvent(event, null, null);
    }

    public void handleSubAgentEvent(AgentEvent event, String agentName, String parentToolCallId) {
        handleEvent(event, agentName, parentToolCallId);
    }

    private void handleEvent(AgentEvent event, String agentName, String parentToolCallId) {
        cancellationToken.throwIfCancelled();
        if (event == null) {
            return;
        }

        EventScope scope = scopes.computeIfAbsent(scopeKey(parentToolCallId, agentName), key -> new EventScope());

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

    private void handleLegacyEvent(Event event, String forcedParentToolCallId, String forcedAgentName) {
        cancellationToken.throwIfCancelled();
        if (event == null) {
            return;
        }

        if (handleForwardedSubAgentEvents(event, forcedParentToolCallId, forcedAgentName)) {
            return;
        }

        boolean subAgentEvent = isLegacySubAgentEvent(event) || StrUtil.isNotBlank(forcedParentToolCallId);
        String agentName = resolveLegacyAgentName(event, forcedAgentName, subAgentEvent);
        String parentToolCallId = subAgentEvent
                ? resolveLegacyParentToolCallId(event, forcedParentToolCallId, agentName)
                : null;
        if (subAgentEvent && StrUtil.isNotBlank(parentToolCallId)) {
            legacyParentToolCallIdsWithChildEvents.add(parentToolCallId);
        }
        EventScope scope = scopes.computeIfAbsent(scopeKey(parentToolCallId, agentName), key -> new EventScope());

        Msg message = event.getMessage();
        if (message != null && message.getContent() != null) {
            for (ContentBlock block : message.getContent()) {
                if (block instanceof ThinkingBlock thinkingBlock) {
                    handleLegacyThinkingBlock(scope, agentName, parentToolCallId, thinkingBlock);
                } else if (block instanceof TextBlock textBlock) {
                    handleLegacyTextBlock(scope, agentName, parentToolCallId, textBlock, event);
                } else if (block instanceof ToolUseBlock toolUseBlock) {
                    handleLegacyToolUseBlock(scope, agentName, parentToolCallId, toolUseBlock);
                } else if (block instanceof ToolResultBlock toolResultBlock) {
                    handleLegacyToolResultBlock(scope, agentName, parentToolCallId, toolResultBlock);
                }
            }
        }

        if (subAgentEvent && event.isLast() && event.getType() == EventType.AGENT_RESULT) {
            emitSubAgentFinished(agentName, parentToolCallId);
        }
    }

    private boolean handleForwardedSubAgentEvents(Event event,
            String inheritedParentToolCallId,
            String inheritedAgentName) {
        Msg message = event.getMessage();
        if (message == null || message.getContent() == null) {
            return false;
        }

        boolean forwarded = false;
        for (ContentBlock block : message.getContent()) {
            if (!(block instanceof ToolResultBlock toolResultBlock)) {
                continue;
            }
            Map<String, Object> metadata = toolResultBlock.getMetadata();
            if (metadata == null || !metadata.containsKey(SUBAGENT_EVENT_METADATA)) {
                continue;
            }
            Event childEvent = extractForwardedEvent(metadata.get(SUBAGENT_EVENT_METADATA));
            if (childEvent == null) {
                continue;
            }

            String agentName = StrUtil.blankToDefault(
                    metadataString(metadata, SUBAGENT_NAME_METADATA),
                    StrUtil.blankToDefault(
                            metadataString(metadata, SUBAGENT_ID_METADATA),
                            resolveLegacyAgentName(childEvent, inheritedAgentName, true)));
            String parentToolCallId = resolveForwardedParentToolCallId(
                    toolResultBlock, childEvent, inheritedParentToolCallId, metadata, agentName);
            handleLegacyEvent(childEvent, parentToolCallId, agentName);
            forwarded = true;
        }
        return forwarded;
    }

    private Event extractForwardedEvent(Object rawEvent) {
        if (rawEvent instanceof Event event) {
            return event;
        }
        return null;
    }

    private String resolveForwardedParentToolCallId(ToolResultBlock wrapper,
            Event childEvent,
            String inheritedParentToolCallId,
            Map<String, Object> metadata,
            String agentName) {
        if (StrUtil.isNotBlank(inheritedParentToolCallId)) {
            return inheritedParentToolCallId;
        }
        if (StrUtil.isNotBlank(wrapper.getId())) {
            return wrapper.getId();
        }
        String sessionId = metadataString(metadata, SUBAGENT_SESSION_ID_METADATA);
        if (StrUtil.isNotBlank(sessionId)) {
            String mapped = legacySessionIdToParentToolCallId.get(sessionId);
            if (StrUtil.isNotBlank(mapped)) {
                return mapped;
            }
        }
        return resolveLegacyParentToolCallId(childEvent, null, agentName);
    }

    private void handleLegacyThinkingBlock(EventScope scope,
            String agentName,
            String parentToolCallId,
            ThinkingBlock thinkingBlock) {
        String thinking = thinkingBlock.getThinking();
        if (StrUtil.isNotBlank(thinking)) {
            handleThinkingDelta(scope, agentName, parentToolCallId, thinking);
        }
    }

    private void handleLegacyTextBlock(EventScope scope,
            String agentName,
            String parentToolCallId,
            TextBlock textBlock,
            Event event) {
        String text = textBlock.getText();
        if (StrUtil.isBlank(text)) {
            return;
        }

        String delta = legacyContentDelta(scope, text, event);
        if (StrUtil.isBlank(delta)) {
            return;
        }

        handleTextDelta(scope, agentName, parentToolCallId, delta);
        scope.legacyContent.append(delta);
    }

    private String legacyContentDelta(EventScope scope, String incomingText, Event event) {
        String emittedText = scope.legacyContent.toString();
        if (StrUtil.isBlank(emittedText)) {
            return incomingText;
        }

        // 旧 stream() 可能同时发送增量 TextBlock 和最终/总结 TextBlock：
        // - 增量块通常是 token/chunk；
        // - 最终 AGENT_RESULT / SUMMARY 可能再次携带已输出全文或最后一小段。
        // 这里按已发文本计算真正新增的 delta，避免前端 timeline 出现重复段落。
        // 待 AgentScope v2 的 streamEvents() 完整透传子 Agent 事件后，本旧 Event
        // 兼容层会删除，这段去重也会随迁移一并移除。
        if (incomingText.equals(emittedText) || emittedText.endsWith(incomingText)) {
            return "";
        }

        if (incomingText.startsWith(emittedText)) {
            return incomingText.substring(emittedText.length());
        }

        boolean aggregateTextEvent = event.isLast()
                || event.getType() == EventType.AGENT_RESULT
                || event.getType() == EventType.SUMMARY;
        if (!aggregateTextEvent) {
            return incomingText;
        }

        if (emittedText.contains(incomingText)
                || normalizeLegacyText(emittedText).contains(normalizeLegacyText(incomingText))) {
            return "";
        }

        int overlapLength = commonSuffixPrefixLength(emittedText, incomingText);
        int minimumUsefulOverlap = Math.min(24, Math.min(emittedText.length(), incomingText.length()));
        if (overlapLength >= Math.max(8, minimumUsefulOverlap)) {
            return incomingText.substring(overlapLength);
        }

        String prefixTrimmed = dropLeadingRepeatedLegacySegments(emittedText, incomingText);
        return collapseNearDuplicateSummarySegments(prefixTrimmed);
    }

    private String normalizeLegacyText(String text) {
        if (text == null) {
            return "";
        }
        return text.replaceAll("\\s+", "");
    }

    private int commonSuffixPrefixLength(String left, String right) {
        if (StrUtil.isBlank(left) || StrUtil.isBlank(right)) {
            return 0;
        }
        int max = Math.min(left.length(), right.length());
        for (int length = max; length > 0; length--) {
            if (left.regionMatches(left.length() - length, right, 0, length)) {
                return length;
            }
        }
        return 0;
    }

    private String dropLeadingRepeatedLegacySegments(String emittedText, String incomingText) {
        String remaining = incomingText;
        boolean droppedAny = false;
        while (true) {
            LegacyTextSegment segment = firstLegacyTextSegment(remaining);
            if (segment == null || StrUtil.isBlank(segment.text())) {
                return droppedAny ? "" : incomingText;
            }
            if (!isRepeatedLegacySegment(emittedText, segment.text())) {
                break;
            }
            droppedAny = true;
            remaining = remaining.substring(segment.endOffset());
        }
        return droppedAny ? remaining.stripLeading() : incomingText;
    }

    private LegacyTextSegment firstLegacyTextSegment(String text) {
        if (StrUtil.isBlank(text)) {
            return null;
        }
        int start = 0;
        while (start < text.length() && Character.isWhitespace(text.charAt(start))) {
            start++;
        }
        if (start >= text.length()) {
            return null;
        }

        int newline = text.indexOf('\n', start);
        int sentenceEnd = firstSentenceBoundary(text, start);
        int end;
        if (newline >= 0 && sentenceEnd >= 0) {
            end = Math.min(newline + 1, sentenceEnd);
        } else if (newline >= 0) {
            end = newline + 1;
        } else if (sentenceEnd >= 0) {
            end = sentenceEnd;
        } else {
            end = text.length();
        }
        return new LegacyTextSegment(text.substring(start, end).trim(), end);
    }

    private int firstSentenceBoundary(String text, int start) {
        for (int index = start; index < text.length(); index++) {
            char ch = text.charAt(index);
            if (ch == '。' || ch == '！' || ch == '？' || ch == '.' || ch == '!' || ch == '?') {
                return index + 1;
            }
        }
        return -1;
    }

    private boolean isRepeatedLegacySegment(String emittedText, String candidate) {
        String normalizedCandidate = normalizeLegacyComparableText(candidate);
        if (normalizedCandidate.length() < 6) {
            return false;
        }
        String normalizedEmitted = normalizeLegacyComparableText(emittedText);
        if (normalizedEmitted.contains(normalizedCandidate)) {
            return true;
        }
        for (String emittedSegment : emittedText.split("\\R+")) {
            String normalizedSegment = normalizeLegacyComparableText(emittedSegment);
            if (normalizedSegment.length() < 6) {
                continue;
            }
            double similarity = legacySimilarity(normalizedCandidate, normalizedSegment);
            if (similarity >= 0.82) {
                return true;
            }
        }
        return false;
    }

    private String collapseNearDuplicateSummarySegments(String text) {
        if (StrUtil.isBlank(text) || !text.contains("汇总")) {
            return text;
        }

        String[] lines = text.split("\\R", -1);
        StringBuilder collapsed = new StringBuilder();
        String previousSummary = null;
        int previousSummaryStart = -1;
        for (String line : lines) {
            String trimmed = line.trim();
            boolean summaryLine = trimmed.startsWith("汇总");
            if (summaryLine && previousSummary != null
                    && legacySimilarity(
                            normalizeLegacyComparableText(previousSummary),
                            normalizeLegacyComparableText(trimmed)) >= 0.72) {
                collapsed.setLength(previousSummaryStart);
            }
            if (summaryLine) {
                previousSummary = trimmed;
                previousSummaryStart = collapsed.length();
            }
            collapsed.append(line).append('\n');
        }
        if (collapsed.length() > 0) {
            collapsed.setLength(collapsed.length() - 1);
        }
        return collapsed.toString();
    }

    private String normalizeLegacyComparableText(String text) {
        if (text == null) {
            return "";
        }
        return text
                .replaceAll("\\s+", "")
                .replaceAll("[`*_#>\\-•·:：,，;；()（）\\[\\]【】\"'“”‘’]", "");
    }

    private double legacySimilarity(String left, String right) {
        if (StrUtil.isBlank(left) || StrUtil.isBlank(right)) {
            return 0;
        }
        int lcs = longestCommonSubsequenceLength(left, right);
        return lcs / (double) Math.min(left.length(), right.length());
    }

    private int longestCommonSubsequenceLength(String left, String right) {
        int[] previous = new int[right.length() + 1];
        int[] current = new int[right.length() + 1];
        for (int i = 1; i <= left.length(); i++) {
            char leftChar = left.charAt(i - 1);
            for (int j = 1; j <= right.length(); j++) {
                if (leftChar == right.charAt(j - 1)) {
                    current[j] = previous[j - 1] + 1;
                } else {
                    current[j] = Math.max(previous[j], current[j - 1]);
                }
            }
            int[] tmp = previous;
            previous = current;
            current = tmp;
        }
        return previous[right.length()];
    }

    private void handleLegacyToolUseBlock(EventScope scope,
            String agentName,
            String parentToolCallId,
            ToolUseBlock toolUseBlock) {
        String toolCallId = normalizeToolCallId(toolUseBlock.getId(), toolUseBlock.getName());
        ToolCallAccumulator toolCall = scope.toolCall(toolCallId);
        toolCall.name = resolveLegacyToolDisplayName(toolUseBlock, toolCall.name);
        toolCall.arguments.setLength(0);
        toolCall.arguments.append(JSONUtil.toJsonStr(toolUseBlock.getInput()));

        emitToolCall(scope, agentName, parentToolCallId, toolCallId);
        if (StrUtil.isBlank(parentToolCallId)) {
            trackLegacyRootToolCall(toolCallId, toolUseBlock);
        }
    }

    private void handleLegacyToolResultBlock(EventScope scope,
            String agentName,
            String parentToolCallId,
            ToolResultBlock toolResultBlock) {
        Map<String, Object> metadata = toolResultBlock.getMetadata();
        if (metadata != null && metadata.containsKey(SUBAGENT_EVENT_METADATA)) {
            return;
        }

        String toolCallId = normalizeToolCallId(toolResultBlock.getId(), toolResultBlock.getName());
        if (StrUtil.isBlank(toolCallId) || "unknown".equals(toolCallId)) {
            return;
        }

        ToolCallAccumulator toolCall = scope.toolCall(toolCallId);
        toolCall.name = resolveLegacyResultToolDisplayName(toolResultBlock.getName(), toolCall.name);
        String resultText = contentBlocksToText(toolResultBlock.getOutput());
        toolCall.result.setLength(0);
        if (!shouldSuppressLegacySubAgentToolResult(parentToolCallId, toolResultBlock.getName(), toolCallId)) {
            toolCall.result.append(resultText);
        }

        emitToolFinished(scope, agentName, parentToolCallId, toolCallId, toolResultBlock.getState());
        if (StrUtil.isBlank(parentToolCallId)) {
            completeLegacyRootToolCall(toolCallId, toolCall.name, resultText);
        }
    }

    private boolean shouldSuppressLegacySubAgentToolResult(String parentToolCallId,
            String rawToolName,
            String toolCallId) {
        return StrUtil.isBlank(parentToolCallId)
                && isGenericSubAgentTool(rawToolName)
                && legacyParentToolCallIdsWithChildEvents.contains(toolCallId);
    }

    private boolean isLegacySubAgentEvent(Event event) {
        EventSource source = event.getSource();
        return source != null
                && (source.getDepth() > 0 || StrUtil.isNotBlank(source.getParentSessionId()));
    }

    private String resolveLegacyAgentName(Event event, String forcedAgentName, boolean subAgentEvent) {
        if (StrUtil.isNotBlank(forcedAgentName)) {
            return forcedAgentName;
        }
        EventSource source = event.getSource();
        if (source != null) {
            String fromSource = StrUtil.blankToDefault(source.getAgentName(), source.getAgentId());
            if (StrUtil.isNotBlank(fromSource)) {
                return fromSource;
            }
        }
        Msg message = event.getMessage();
        if (message != null && StrUtil.isNotBlank(message.getName())) {
            return message.getName();
        }
        return subAgentEvent ? "sub_agent" : mainAgentName;
    }

    private String resolveLegacyParentToolCallId(Event event,
            String forcedParentToolCallId,
            String agentName) {
        if (StrUtil.isNotBlank(forcedParentToolCallId)) {
            return forcedParentToolCallId;
        }

        EventSource source = event.getSource();
        if (source != null) {
            String mappedByKey = legacyAgentKeyToParentToolCallId.get(source.getAgentKey());
            if (StrUtil.isNotBlank(mappedByKey)) {
                return mappedByKey;
            }
            String mappedBySession = legacySessionIdToParentToolCallId.get(source.getSessionId());
            if (StrUtil.isNotBlank(mappedBySession)) {
                return mappedBySession;
            }
            // 并行同类型子 agent 时 agentId 不唯一，跳过此映射走 ensureSyntheticParentToolCall
            if (!legacyAgentIdCollisions.contains(source.getAgentId())) {
                String mappedByAgentId = legacyAgentIdToParentToolCallId.get(source.getAgentId());
                if (StrUtil.isNotBlank(mappedByAgentId)) {
                    return mappedByAgentId;
                }
            }
        }

        // 并行同类型子 agent 时不走 latest fallback，直接创建合成节点以按 sessionId 区分
        boolean agentIdCollision = source != null
                && legacyAgentIdCollisions.contains(source.getAgentId());
        if (!agentIdCollision
                && StrUtil.isNotBlank(latestLegacyRootToolCallId)
                && legacyActiveRootToolCallIds.contains(latestLegacyRootToolCallId)) {
            return latestLegacyRootToolCallId;
        }

        return ensureSyntheticParentToolCall(source, agentName);
    }

    private String ensureSyntheticParentToolCall(EventSource source, String agentName) {
        String sourceKey = source != null
                ? StrUtil.blankToDefault(source.getAgentKey(),
                        StrUtil.blankToDefault(source.getSessionId(),
                                StrUtil.blankToDefault(source.getPath(), source.getAgentId())))
                : agentName;
        String parentToolCallId = "subagent:" + StrUtil.blankToDefault(sourceKey, "unknown");
        if (parentToolCallId.length() > 120) {
            parentToolCallId = StrUtil.sub(parentToolCallId, 0, 120);
        }

        if (legacySyntheticParentToolCalls.putIfAbsent(parentToolCallId, Boolean.TRUE) == null) {
            String toolName = StrUtil.blankToDefault(agentName,
                    source != null ? StrUtil.blankToDefault(source.getAgentId(), "sub_agent") : "sub_agent");
            emitEvent(new AiChatStreamRespVO()
                    .setMessageId(messageId)
                    .setConversationId(conversationId)
                    .setOutputType("TOOL_CALL")
                    .setToolCalls(List.of(new AiChatStreamRespVO.ToolCallVO()
                            .setId(parentToolCallId)
                            .setName(toolName)
                            .setArguments("{}")))
                    .setAgentName(toolName)
                    .setFinished(false));
        }

        return parentToolCallId;
    }

    private void trackLegacyRootToolCall(String toolCallId, ToolUseBlock toolUseBlock) {
        if (StrUtil.isBlank(toolCallId)) {
            return;
        }
        legacyActiveRootToolCallIds.add(toolCallId);
        latestLegacyRootToolCallId = toolCallId;

        String toolName = toolUseBlock.getName();
        if (StrUtil.isNotBlank(toolName)) {
            legacyAgentIdToParentToolCallId.putIfAbsent(toolName, toolCallId);
        }
        Map<String, Object> input = toolUseBlock.getInput();
        if (input != null) {
            indexLegacyToolInput(input, toolCallId);
        }
    }

    private void indexLegacyToolInput(Map<String, Object> input, String toolCallId) {
        String agentId = stringValue(input.get("agent_id"));
        if (StrUtil.isNotBlank(agentId)) {
            String existing = legacyAgentIdToParentToolCallId.putIfAbsent(agentId, toolCallId);
            if (existing != null && !existing.equals(toolCallId)) {
                // 同一 agentId 被多个不同 toolCallId spawn → 标记为并行冲突
                legacyAgentIdCollisions.add(agentId);
            }
        }
        putIfNotBlank(legacyAgentKeyToParentToolCallId, stringValue(input.get("agent_key")), toolCallId);
        putIfNotBlank(legacySessionIdToParentToolCallId, stringValue(input.get("session_id")), toolCallId);
    }

    private void completeLegacyRootToolCall(String toolCallId, String toolName, String resultText) {
        legacyActiveRootToolCallIds.remove(toolCallId);
        if (StrUtil.equals(latestLegacyRootToolCallId, toolCallId)) {
            latestLegacyRootToolCallId = legacyActiveRootToolCallIds.stream().findFirst().orElse(null);
        }
        putIfNotBlank(legacyAgentKeyToParentToolCallId, extractLineValue(resultText, "agent_key"), toolCallId);
        putIfNotBlank(legacyAgentIdToParentToolCallId, extractLineValue(resultText, "agent_id"), toolCallId);
        putIfNotBlank(legacySessionIdToParentToolCallId, extractLineValue(resultText, "session_id"), toolCallId);
        if (StrUtil.isNotBlank(toolName)) {
            legacyAgentIdToParentToolCallId.putIfAbsent(toolName, toolCallId);
        }
    }

    private void putIfNotBlank(ConcurrentHashMap<String, String> map, String key, String value) {
        if (StrUtil.isNotBlank(key) && StrUtil.isNotBlank(value)) {
            map.putIfAbsent(key.trim(), value);
        }
    }

    private String extractLineValue(String text, String key) {
        if (StrUtil.isBlank(text) || StrUtil.isBlank(key)) {
            return null;
        }
        String prefix = key + ":";
        String[] lines = text.split("\\R");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith(prefix)) {
                String value = trimmed.substring(prefix.length()).trim();
                return StrUtil.isBlank(value) ? null : value;
            }
        }
        return null;
    }

    private String contentBlocksToText(List<ContentBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return "";
        }
        StringBuilder text = new StringBuilder();
        for (ContentBlock block : blocks) {
            if (block instanceof TextBlock textBlock) {
                text.append(StrUtil.nullToEmpty(textBlock.getText()));
            } else if (block instanceof ThinkingBlock thinkingBlock) {
                text.append(StrUtil.nullToEmpty(thinkingBlock.getThinking()));
            } else {
                text.append(String.valueOf(block));
            }
        }
        return text.toString();
    }

    private String normalizeToolCallId(String toolCallId, String toolName) {
        if (StrUtil.isNotBlank(toolCallId)) {
            return toolCallId;
        }
        if (StrUtil.isNotBlank(toolName)) {
            return "legacy-tool:" + toolName;
        }
        return "unknown";
    }

    private String resolveLegacyToolDisplayName(ToolUseBlock toolUseBlock, String fallbackName) {
        String rawToolName = toolUseBlock.getName();
        String toolName = StrUtil.blankToDefault(rawToolName, fallbackName);
        Map<String, Object> input = toolUseBlock.getInput();

        if ("agent_spawn".equals(rawToolName)) {
            // Harness 的 agent_spawn 是通用调度工具，真正的子 Agent 名称在参数 agent_id 中。
            // 这里把父工具节点展示名改成具体 agent_id，前端就能显示
            // “分集分镜编写（子Agent）” 这类业务名称，而不是笼统的“子智能体调度”。
            // 未来迁回 streamEvents() 时，如果 v2 能直接提供子 Agent display name，
            // 这段旧 Event 兼容命名逻辑也应一并删除。
            return StrUtil.blankToDefault(firstInputString(input,
                    "agent_id", "agentId", "agent_name", "agentName", "label"), toolName);
        }

        if ("agent_send".equals(rawToolName)) {
            return StrUtil.blankToDefault(firstInputString(input,
                    "label", "agent_id", "agentId", "agent_key", "agentKey"), toolName);
        }

        return toolName;
    }

    private String resolveLegacyResultToolDisplayName(String rawToolName, String existingName) {
        if (isGenericSubAgentTool(rawToolName) && StrUtil.isNotBlank(existingName)
                && !isGenericSubAgentTool(existingName)) {
            return existingName;
        }
        return StrUtil.blankToDefault(rawToolName, existingName);
    }

    private boolean isGenericSubAgentTool(String toolName) {
        return "agent_spawn".equals(toolName) || "agent_send".equals(toolName);
    }

    private String firstInputString(Map<String, Object> input, String... keys) {
        if (input == null) {
            return null;
        }
        for (String key : keys) {
            String value = stringValue(input.get(key));
            if (StrUtil.isNotBlank(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private String metadataString(Map<String, Object> metadata, String key) {
        return metadata != null ? stringValue(metadata.get(key)) : null;
    }

    private String stringValue(Object value) {
        return value != null ? String.valueOf(value) : null;
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
        legacyAgentKeyToParentToolCallId.clear();
        legacyAgentIdToParentToolCallId.clear();
        legacySessionIdToParentToolCallId.clear();
        legacySyntheticParentToolCalls.clear();
        legacyActiveRootToolCallIds.clear();
        legacyParentToolCallIdsWithChildEvents.clear();
        legacyAgentIdCollisions.clear();
        latestLegacyRootToolCallId = null;
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
        if (toolCall.callEmitted) {
            return;
        }
        toolCall.callEmitted = true;
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
        if (toolCall.resultEmitted) {
            return;
        }
        toolCall.resultEmitted = true;
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

    private void emitSubAgentFinished(String agentName, String parentToolCallId) {
        if (StrUtil.isBlank(parentToolCallId)) {
            return;
        }
        emitEvent(new AiChatStreamRespVO()
                .setMessageId(messageId)
                .setConversationId(conversationId)
                .setOutputType("SUB_AGENT_FINISHED")
                .setParentToolCallId(parentToolCallId)
                .setAgentName(agentName)
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

    private String scopeKey(String parentToolCallId, String agentName) {
        String parentKey = StrUtil.blankToDefault(parentToolCallId, MAIN_SCOPE_KEY);
        if (!isSubAgent(agentName)) {
            return parentKey;
        }
        return parentKey + ":" + agentName;
    }

    private boolean isSubAgent(String agentName) {
        return StrUtil.isNotBlank(agentName) && !mainAgentName.equals(agentName);
    }

    private String getAgentKey(Agent agent) {
        return agent.getName() + ":" + System.identityHashCode(agent);
    }

    private static class EventScope {
        private volatile long reasoningStartTime;
        private volatile Long reasoningDurationMs;
        private final StringBuilder legacyContent = new StringBuilder();
        private final ConcurrentHashMap<String, ToolCallAccumulator> toolCalls = new ConcurrentHashMap<>();

        ToolCallAccumulator toolCall(String toolCallId) {
            return toolCalls.computeIfAbsent(StrUtil.blankToDefault(toolCallId, "unknown"), id -> new ToolCallAccumulator());
        }
    }

    private static class ToolCallAccumulator {
        private volatile String name;
        private volatile boolean callEmitted;
        private volatile boolean resultEmitted;
        private final StringBuilder arguments = new StringBuilder();
        private final StringBuilder result = new StringBuilder();
    }

    private record LegacyTextSegment(String text, int endOffset) {
    }
}
