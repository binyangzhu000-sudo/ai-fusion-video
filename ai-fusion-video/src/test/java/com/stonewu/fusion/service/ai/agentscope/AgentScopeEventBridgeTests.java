package com.stonewu.fusion.service.ai.agentscope;

import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import io.agentscope.core.agent.Event;
import io.agentscope.core.agent.EventSource;
import io.agentscope.core.agent.EventType;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ThinkingBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings({"deprecation", "removal"})
class AgentScopeEventBridgeTests {

    @Test
    void convertsThinkingAndTextDeltasToReasoningAndContentEvents() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(new ThinkingBlockDeltaEvent("reply-1", "thinking", "先分析"));
        bridge.handleMainEvent(new TextBlockDeltaEvent("reply-1", "text", "结果"));

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("REASONING");
        assertThat(events.get(0).getReasoningContent()).isEqualTo("先分析");
        assertThat(events.get(1).getOutputType()).isEqualTo("CONTENT");
        assertThat(events.get(1).getContent()).isEqualTo("结果");
        assertThat(events.get(1).getReasoningDurationMs()).isNotNull();
    }

    @Test
    void aggregatesToolCallAndToolResultEvents() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(new ToolCallStartEvent("reply-1", "call-1", "query_asset"));
        bridge.handleMainEvent(new ToolCallDeltaEvent("reply-1", "call-1", "{\"keyword\":"));
        bridge.handleMainEvent(new ToolCallDeltaEvent("reply-1", "call-1", "\"cat\"}"));
        bridge.handleMainEvent(new ToolCallEndEvent("reply-1", "call-1"));
        bridge.handleMainEvent(new ToolResultStartEvent("reply-1", "call-1", "query_asset"));
        bridge.handleMainEvent(new ToolResultTextDeltaEvent("reply-1", "call-1", "{\"status\":\"success\"}"));
        bridge.handleMainEvent(new ToolResultEndEvent("reply-1", "call-1", ToolResultState.SUCCESS));

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("TOOL_CALL");
        assertThat(events.get(0).getToolCalls()).hasSize(1);
        assertThat(events.get(0).getToolCalls().get(0).getName()).isEqualTo("query_asset");
        assertThat(events.get(0).getToolCalls().get(0).getArguments()).isEqualTo("{\"keyword\":\"cat\"}");

        assertThat(events.get(1).getOutputType()).isEqualTo("TOOL_FINISHED");
        assertThat(events.get(1).getToolName()).isEqualTo("query_asset");
        assertThat(events.get(1).getToolStatus()).isEqualTo("success");
        assertThat(events.get(1).getToolResult()).isEqualTo("{\"status\":\"success\"}");
    }

    @Test
    void marksSubAgentEventsWithParentToolCallId() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleSubAgentEvent(new TextBlockDeltaEvent("reply-2", "text", "子任务输出"),
                "storyboard_agent", "parent-call-1");
        bridge.handleSubAgentEvent(new AgentEndEvent("reply-2"),
                "storyboard_agent", "parent-call-1");

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("CONTENT");
        assertThat(events.get(0).getAgentName()).isEqualTo("storyboard_agent");
        assertThat(events.get(0).getParentToolCallId()).isEqualTo("parent-call-1");
        assertThat(events.get(1).getOutputType()).isEqualTo("SUB_AGENT_FINISHED");
        assertThat(events.get(1).getAgentName()).isEqualTo("storyboard_agent");
        assertThat(events.get(1).getParentToolCallId()).isEqualTo("parent-call-1");
    }

    @Test
    void convertsLegacyThinkingAndTextEvents() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(legacyEvent(EventType.REASONING, false,
                ThinkingBlock.builder().thinking("旧推理").build()));
        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, true,
                TextBlock.builder().text("旧正文").build()));

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("REASONING");
        assertThat(events.get(0).getReasoningContent()).isEqualTo("旧推理");
        assertThat(events.get(0).getAgentName()).isNull();
        assertThat(events.get(1).getOutputType()).isEqualTo("CONTENT");
        assertThat(events.get(1).getContent()).isEqualTo("旧正文");
        assertThat(events.get(1).getAgentName()).isNull();
    }

    @Test
    void suppressesDuplicateLegacyAggregateText() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);
        List<AiChatStreamRespVO> events = capture(sink);

        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, false,
                TextBlock.builder().text("首次任务超时出错，重新调度...").build()));
        bridge.handleMainEvent(legacyEvent(EventType.SUMMARY, true,
                TextBlock.builder().text("首次任务超时出错，重新调度...").build()));

        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getOutputType()).isEqualTo("CONTENT");
        assertThat(events.getFirst().getContent()).isEqualTo("首次任务超时出错，重新调度...");
    }

    @Test
    void emitsOnlyNewSuffixFromLegacyAggregateText() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);
        List<AiChatStreamRespVO> events = capture(sink);

        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, false,
                TextBlock.builder().text("开始调度").build()));
        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, true,
                TextBlock.builder().text("开始调度，等待结果").build()));

        assertThat(events).hasSize(2);
        assertThat(events.get(0).getContent()).isEqualTo("开始调度");
        assertThat(events.get(1).getContent()).isEqualTo("，等待结果");
    }

    @Test
    void emitsOnlyNewSuffixWhenLegacyAggregateRepeatsAfterToolOutput() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);
        List<AiChatStreamRespVO> events = capture(sink);

        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, false,
                TextBlock.builder().text("我将先查询子资产信息。").build()));
        bridge.handleMainEvent(legacyEvent(EventType.AGENT_RESULT, true,
                TextBlock.builder().text("我将先查询子资产信息。接下来生成图片。").build()));

        assertThat(events).hasSize(2);
        assertThat(events.get(0).getContent()).isEqualTo("我将先查询子资产信息。");
        assertThat(events.get(1).getContent()).isEqualTo("接下来生成图片。");
    }

    @Test
    void convertsLegacyToolUseAndToolResultEvents() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(legacyEvent(EventType.REASONING, false,
                ToolUseBlock.builder()
                        .id("call-legacy")
                        .name("query_project")
                        .input(Map.of("projectId", 12))
                        .build()));
        bridge.handleMainEvent(legacyEvent(EventType.TOOL_RESULT, false,
                ToolResultBlock.builder()
                        .id("call-legacy")
                        .name("query_project")
                        .output(TextBlock.builder().text("{\"status\":\"success\"}").build())
                        .state(ToolResultState.SUCCESS)
                        .build()));

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("TOOL_CALL");
        assertThat(events.get(0).getToolCalls()).hasSize(1);
        assertThat(events.get(0).getToolCalls().get(0).getId()).isEqualTo("call-legacy");
        assertThat(events.get(0).getToolCalls().get(0).getName()).isEqualTo("query_project");
        assertThat(events.get(0).getToolCalls().get(0).getArguments()).contains("\"projectId\":12");

        assertThat(events.get(1).getOutputType()).isEqualTo("TOOL_FINISHED");
        assertThat(events.get(1).getToolCallId()).isEqualTo("call-legacy");
        assertThat(events.get(1).getToolName()).isEqualTo("query_project");
        assertThat(events.get(1).getToolStatus()).isEqualTo("success");
        assertThat(events.get(1).getToolResult()).isEqualTo("{\"status\":\"success\"}");
    }

    @Test
    void usesAgentIdAsLegacyAgentSpawnDisplayName() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(legacyEvent(EventType.REASONING, false,
                ToolUseBlock.builder()
                        .id("spawn-call-1")
                        .name("agent_spawn")
                        .input(Map.of("agent_id", "episode_storyboard_writer"))
                        .build()));
        bridge.handleMainEvent(legacyEvent(EventType.TOOL_RESULT, false,
                ToolResultBlock.builder()
                        .id("spawn-call-1")
                        .name("agent_spawn")
                        .output(TextBlock.builder().text("status: ok").build())
                        .state(ToolResultState.SUCCESS)
                        .build()));

        List<AiChatStreamRespVO> events = collect(sink, 2);

        assertThat(events.get(0).getOutputType()).isEqualTo("TOOL_CALL");
        assertThat(events.get(0).getToolCalls().get(0).getName())
                .isEqualTo("episode_storyboard_writer");
        assertThat(events.get(1).getOutputType()).isEqualTo("TOOL_FINISHED");
        assertThat(events.get(1).getToolName()).isEqualTo("episode_storyboard_writer");
    }

    @Test
    void routesLegacySubAgentContentToParentToolCall() {
        Sinks.Many<AiChatStreamRespVO> sink = Sinks.many().unicast().onBackpressureBuffer();
        AgentScopeEventBridge bridge = newBridge(sink);

        bridge.handleMainEvent(legacyEvent(EventType.REASONING, false,
                ToolUseBlock.builder()
                        .id("spawn-call-1")
                        .name("episode_writer")
                        .input(Map.of("agent_id", "episode_writer_agent"))
                        .build()));
        bridge.handleMainEvent(legacySubAgentEvent(EventType.AGENT_RESULT, true,
                "episode_writer_agent",
                TextBlock.builder().text("子 agent 文本").build()));

        List<AiChatStreamRespVO> events = collect(sink, 3);

        assertThat(events.get(0).getOutputType()).isEqualTo("TOOL_CALL");
        assertThat(events.get(0).getToolCalls().get(0).getId()).isEqualTo("spawn-call-1");
        assertThat(events.get(1).getOutputType()).isEqualTo("CONTENT");
        assertThat(events.get(1).getContent()).isEqualTo("子 agent 文本");
        assertThat(events.get(1).getParentToolCallId()).isEqualTo("spawn-call-1");
        assertThat(events.get(1).getAgentName()).isEqualTo("episode_writer_agent");
        assertThat(events.get(2).getOutputType()).isEqualTo("SUB_AGENT_FINISHED");
        assertThat(events.get(2).getParentToolCallId()).isEqualTo("spawn-call-1");
        assertThat(events.get(2).getAgentName()).isEqualTo("episode_writer_agent");
        assertThat(events.get(2).getFinished()).isFalse();
    }

    private AgentScopeEventBridge newBridge(Sinks.Many<AiChatStreamRespVO> sink) {
        return new AgentScopeEventBridge(
                sink, "conversation-1", "message-1", "main_agent",
                new AgentCancellationToken(() -> false));
    }

    private Event legacyEvent(EventType type, boolean last, ContentBlock... blocks) {
        return new Event(type, legacyMessage("main_agent", blocks), last);
    }

    private Event legacySubAgentEvent(EventType type,
            boolean last,
            String agentName,
            ContentBlock... blocks) {
        EventSource source = EventSource.builder()
                .agentKey(agentName + "-key")
                .agentId(agentName)
                .agentName(agentName)
                .sessionId(agentName + "-session")
                .parentSessionId("conversation-1")
                .depth(1)
                .path("main_agent/" + agentName)
                .build();
        return new Event(type, legacyMessage(agentName, blocks), last, source);
    }

    private Msg legacyMessage(String name, ContentBlock... blocks) {
        return Msg.builder()
                .name(name)
                .role(MsgRole.ASSISTANT)
                .content(List.of(blocks))
                .build();
    }

    private List<AiChatStreamRespVO> capture(Sinks.Many<AiChatStreamRespVO> sink) {
        List<AiChatStreamRespVO> events = new ArrayList<>();
        sink.asFlux().subscribe(events::add);
        return events;
    }

    private List<AiChatStreamRespVO> collect(Sinks.Many<AiChatStreamRespVO> sink, int count) {
        return sink.asFlux()
                .take(count)
                .collectList()
                .block(Duration.ofSeconds(1));
    }
}
