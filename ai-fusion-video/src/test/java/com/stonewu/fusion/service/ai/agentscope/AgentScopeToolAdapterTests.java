package com.stonewu.fusion.service.ai.agentscope;

import com.stonewu.fusion.service.ai.ToolExecutionContext;
import com.stonewu.fusion.service.ai.ToolExecutor;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.tool.ToolBase;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.tool.ToolCallParam;
import io.agentscope.harness.agent.filesystem.AbstractFilesystem;
import io.agentscope.harness.agent.filesystem.spec.LocalFilesystemSpec;
import io.agentscope.harness.agent.workspace.LocalFsMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AgentScopeToolAdapterTests {

    @TempDir
    Path tempDir;

    @Test
    void callAsyncShouldPreserveToolCallIdAndName() {
        ToolExecutor toolExecutor = mock(ToolExecutor.class);
        ToolExecutionContext toolContext = ToolExecutionContext.builder().build();
        when(toolExecutor.getToolName()).thenReturn("asset_query");
        when(toolExecutor.getToolDescription()).thenReturn("query asset");
        when(toolExecutor.getParametersSchema()).thenReturn("{}");
        when(toolExecutor.execute(eq("{\"keyword\":\"cat\"}"), any(ToolExecutionContext.class)))
                .thenReturn("ok");

        AgentScopeToolAdapter adapter = new AgentScopeToolAdapter(
                toolExecutor,
                toolContext,
                new AgentCancellationToken(() -> false));

        ToolUseBlock toolUseBlock = ToolUseBlock.builder()
                .id("call-123")
                .name("asset_query")
                .input(Map.of("keyword", "cat"))
                .build();

        ToolCallParam param = ToolCallParam.builder()
                .toolUseBlock(toolUseBlock)
                .input(Map.of("keyword", "cat"))
                .build();

        ToolResultBlock result = adapter.callAsync(param).block();

        assertEquals("call-123", result.getId());
        assertEquals("asset_query", result.getName());
                TextBlock output = assertInstanceOf(TextBlock.class, result.getOutput().getFirst());
                assertEquals("ok", output.getText());
    }

    @Test
    void shouldExposeReadOnlyMetadataFromToolExecutor() {
        ToolExecutor toolExecutor = mock(ToolExecutor.class);
        when(toolExecutor.getToolName()).thenReturn("get_project_script");
        when(toolExecutor.getToolDescription()).thenReturn("query script");
        when(toolExecutor.getParametersSchema()).thenReturn("{}");
        when(toolExecutor.isReadOnly()).thenReturn(true);

        AgentScopeToolAdapter adapter = new AgentScopeToolAdapter(
                toolExecutor,
                ToolExecutionContext.builder().build(),
                new AgentCancellationToken(() -> false));

        assertTrue(adapter.isReadOnly());
    }

    @Test
    void readOnlyWorkspaceToolsShouldExposeReadOnlyMetadata() {
        AbstractFilesystem filesystem = new LocalFilesystemSpec()
                .project(tempDir)
                .mode(LocalFsMode.SANDBOXED)
                .toFilesystem(tempDir, rc -> List.of());
        Toolkit toolkit = new Toolkit();

        toolkit.registerTool(new AgentScopeReadOnlyWorkspaceTools(filesystem));

        assertTrue(((ToolBase) toolkit.getTool("read_file")).isReadOnly());
        assertTrue(((ToolBase) toolkit.getTool("memory_search")).isReadOnly());
        assertTrue(((ToolBase) toolkit.getTool("session_search")).isReadOnly());
    }

    @Test
    void readOnlyWorkspaceToolsShouldReadHarnessMemoryAndSessionArtifacts() throws Exception {
        Files.writeString(tempDir.resolve("MEMORY.md"), "Project prefers noir lighting\n", StandardCharsets.UTF_8);
        Files.createDirectories(tempDir.resolve("memory"));
        Files.writeString(tempDir.resolve("memory").resolve("facts.md"),
                "Reusable fact: shot A uses a cat mascot\n", StandardCharsets.UTF_8);

        Path sessionDir = tempDir.resolve("agents").resolve("main_agent").resolve("sessions");
        Files.createDirectories(sessionDir);
        Files.writeString(sessionDir.resolve("sessions.json"),
                "{\"sessions\":{\"s1\":{\"title\":\"smoke\"}}}", StandardCharsets.UTF_8);
        Files.writeString(sessionDir.resolve("s1.log.jsonl"),
                "{\"role\":\"user\",\"content\":\"find cat mascot\"}\n"
                        + "{\"role\":\"assistant\",\"content\":\"found it\"}\n",
                StandardCharsets.UTF_8);

        AbstractFilesystem filesystem = new LocalFilesystemSpec()
                .project(tempDir)
                .mode(LocalFsMode.SANDBOXED)
                .toFilesystem(tempDir, rc -> List.of());
        AgentScopeReadOnlyWorkspaceTools tools = new AgentScopeReadOnlyWorkspaceTools(filesystem);

        assertTrue(tools.memorySearch(null, "noir").contains("MEMORY.md"));
        assertTrue(tools.memorySearch(null, "mascot").contains("facts.md"));
        assertTrue(tools.memoryGet(null, "MEMORY.md", 1, 1).contains("noir lighting"));
        assertTrue(tools.sessionList(null, "main_agent").contains("s1"));
        assertTrue(tools.sessionHistory(null, "main_agent", "s1", 2).contains("found it"));
        assertTrue(tools.sessionSearch(null, "cat mascot", null, 5).contains("s1.log.jsonl"));
    }

    @Test
    void readOnlyWorkspaceToolsShouldReadUserNamespaceWithRootFallback() throws Exception {
        Files.writeString(tempDir.resolve("MEMORY.md"), "Root fallback memory\n", StandardCharsets.UTF_8);
        Files.createDirectories(tempDir.resolve("memory"));
        Files.writeString(tempDir.resolve("memory").resolve("root.md"),
                "root-only fact\n", StandardCharsets.UTF_8);

        Path userMemoryDir = tempDir.resolve("7").resolve("memory");
        Files.createDirectories(userMemoryDir);
        Files.writeString(userMemoryDir.resolve("user.md"),
                "user-only fact\n", StandardCharsets.UTF_8);
        Path userSessionDir = tempDir.resolve("7").resolve("agents").resolve("main_agent").resolve("sessions");
        Files.createDirectories(userSessionDir);
        Files.writeString(userSessionDir.resolve("s2.log.jsonl"),
                "{\"role\":\"user\",\"content\":\"namespaced-session\"}\n", StandardCharsets.UTF_8);

        AbstractFilesystem namespacedFilesystem = new LocalFilesystemSpec()
                .project(tempDir)
                .mode(LocalFsMode.SANDBOXED)
                .toFilesystem(tempDir, rc -> List.of(rc.getUserId()));
        AbstractFilesystem rootFallbackFilesystem = new LocalFilesystemSpec()
                .project(tempDir)
                .mode(LocalFsMode.SANDBOXED)
                .toFilesystem(tempDir, rc -> List.of());
        AgentScopeReadOnlyWorkspaceTools tools =
                new AgentScopeReadOnlyWorkspaceTools(namespacedFilesystem, rootFallbackFilesystem);
        RuntimeContext userContext = RuntimeContext.builder().userId("7").build();

        assertTrue(tools.readFile(userContext, "MEMORY.md", 0, 0).contains("Root fallback memory"));
        assertTrue(tools.memorySearch(userContext, "user-only").contains("user.md"));
        assertTrue(tools.memorySearch(userContext, "root-only").contains("root.md"));
        assertTrue(tools.sessionSearch(userContext, "namespaced-session", "main_agent", 5)
                .contains("s2.log.jsonl"));
    }
}
