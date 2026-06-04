package com.stonewu.fusion.service.ai.agentscope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.middleware.ActingInput;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.tool.ToolCallParam;
import io.agentscope.harness.agent.middleware.PlanModeMiddleware;
import io.agentscope.harness.agent.tool.PlanModeTools;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import io.agentscope.harness.agent.workspace.plan.PlanModeManager;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

class AgentScopePlanModeTests {

    @TempDir
    Path tempDir;

    @Test
    void planWriteShouldPersistPlanMarkdownUnderWorkspacePlans() throws Exception {
        PlanModeManager manager = new PlanModeManager(new WorkspaceManager(tempDir), "plans");
        AgentState state = AgentState.builder().replyId("reply-1").build();
        Agent agent = agentWithState(state);
        manager.enter(state);

        PlanModeTools.PlanWriteTool planWrite = new PlanModeTools.PlanWriteTool(manager);
        ToolCallParam param = ToolCallParam.builder()
                .agent(agent)
                .runtimeContext(RuntimeContext.builder()
                        .userId("7")
                        .sessionId("conversation-1")
                        .build())
                .toolUseBlock(new ToolUseBlock("call-plan", "plan_write",
                        Map.of("content", "# Plan\n\n1. Inspect.\n")))
                .input(Map.of("content", "# Plan\n\n1. Inspect.\n"))
                .build();

        ToolResultBlock result = planWrite.callAsync(param).block();

        assertTrue(Files.readString(tempDir.resolve("plans").resolve("PLAN.md"), StandardCharsets.UTF_8)
                .contains("1. Inspect."));
        TextBlock output = (TextBlock) result.getOutput().getFirst();
        assertTrue(output.getText().contains("plans/PLAN.md"));
    }

    @Test
    void planModeShouldDenyMutatingToolsAndResumeThemInBuildMode() {
        PlanModeManager manager = new PlanModeManager(new WorkspaceManager(tempDir), "plans");
        AgentState state = AgentState.builder().replyId("reply-1").build();
        Agent agent = agentWithState(state);
        PlanModeMiddleware middleware = new PlanModeMiddleware(manager, "read_file"::equals);

        manager.enter(state);
        ToolUseBlock readCall = new ToolUseBlock("call-read", "read_file", Map.of("path", "MEMORY.md"));
        ToolUseBlock writeCall = new ToolUseBlock("call-write", "save_script_episode", Map.of("id", 1));
        AtomicReference<List<ToolUseBlock>> allowedInPlan = new AtomicReference<>();

        List<AgentEvent> deniedEvents = middleware.onActing(
                        agent,
                        new ActingInput(List.of(readCall, writeCall)),
                        input -> {
                            allowedInPlan.set(input.toolCalls());
                            return Flux.empty();
                        })
                .collectList()
                .block();

        assertEquals(List.of(readCall), allowedInPlan.get());
        ToolResultEndEvent deniedEnd = (ToolResultEndEvent) deniedEvents.stream()
                .filter(ToolResultEndEvent.class::isInstance)
                .findFirst()
                .orElseThrow();
        assertEquals("call-write", deniedEnd.getToolCallId());
        assertEquals(ToolResultState.DENIED, deniedEnd.getState());

        manager.exit(state);
        AtomicReference<List<ToolUseBlock>> allowedInBuild = new AtomicReference<>();
        List<AgentEvent> buildEvents = middleware.onActing(
                        agent,
                        new ActingInput(List.of(writeCall)),
                        input -> {
                            allowedInBuild.set(input.toolCalls());
                            return Flux.empty();
                        })
                .collectList()
                .block();

        assertEquals(List.of(writeCall), allowedInBuild.get());
        assertTrue(buildEvents.isEmpty());
    }

    private Agent agentWithState(AgentState state) {
        Agent agent = mock(Agent.class);
        when(agent.getAgentState()).thenReturn(state);
        when(agent.getName()).thenReturn("main_agent");
        return agent;
    }
}
