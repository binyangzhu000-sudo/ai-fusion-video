package com.stonewu.fusion.service.ai.agentscope;

import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultStartEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.ToolResultState;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

    private AgentScopeEventBridge newBridge(Sinks.Many<AiChatStreamRespVO> sink) {
        return new AgentScopeEventBridge(
                sink, "conversation-1", "message-1", "main_agent",
                new AgentCancellationToken(() -> false));
    }

    private List<AiChatStreamRespVO> collect(Sinks.Many<AiChatStreamRespVO> sink, int count) {
        return sink.asFlux()
                .take(count)
                .collectList()
                .block(Duration.ofSeconds(1));
    }
}
