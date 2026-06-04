package com.stonewu.fusion.service.ai.agentscope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.stonewu.fusion.config.ai.AiAgentDefinition;
import com.stonewu.fusion.controller.ai.vo.AiChatReqVO;
import com.stonewu.fusion.service.ai.AiAgentService;
import com.stonewu.fusion.service.ai.AiToolConfigService;
import com.stonewu.fusion.service.ai.ToolExecutor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AgentScopeHarnessWorkspaceServiceTests {

    @TempDir
    Path tempDir;

    @Test
    void resolveWorkspacePathShouldPreferProjectNamespaceAndInitializeLayout() {
        AgentScopeHarnessWorkspaceService service = newService();

        Path workspace = service.resolveWorkspacePath(42L, 7L);
        assertEquals(tempDir.resolve("projects").resolve("42").normalize(), workspace);

        service.initializeWorkspace(workspace);

        assertTrue(Files.isRegularFile(workspace.resolve("AGENTS.md")));
        assertTrue(Files.isRegularFile(workspace.resolve("MEMORY.md")));
        assertTrue(Files.isDirectory(workspace.resolve("skills")));
        assertTrue(Files.isDirectory(workspace.resolve("subagents")));
        assertTrue(Files.isDirectory(workspace.resolve("plans")));
    }

    @Test
    void prepareWorkspaceShouldGenerateSubagentSpecAndKeepUserEditedFile() throws Exception {
        AiAgentService aiAgentService = mock(AiAgentService.class);
        AiToolConfigService aiToolConfigService = mock(AiToolConfigService.class);
        AgentScopeHarnessWorkspaceService service =
                newService(aiAgentService, aiToolConfigService);

        when(aiAgentService.getByType("episode_scene_writer"))
                .thenReturn(AiAgentDefinition.builder()
                        .type("episode_scene_writer")
                        .enableTools(1)
                        .build());
        ToolExecutor readTool = mock(ToolExecutor.class);
        when(readTool.getToolName()).thenReturn("get_script_episode");
        ToolExecutor writeTool = mock(ToolExecutor.class);
        when(writeTool.getToolName()).thenReturn("save_script_scene");
        when(aiToolConfigService.getEnabledToolsByAgent("episode_scene_writer"))
                .thenReturn(List.of(readTool, writeTool));

        AiAgentDefinition.SubAgentToolDef subAgent =
                AiAgentDefinition.SubAgentToolDef.builder()
                        .toolName("episode_scene_writer")
                        .displayName("Episode scene writer")
                        .description("Parse one episode into script scenes.")
                        .parametersSchema("{\"type\":\"object\"}")
                        .outputSchema("{\"type\":\"object\"}")
                        .refAgentType("episode_scene_writer")
                        .systemPromptOverride("Handle project {projectId}.")
                        .build();

        Path workspace = service.prepareWorkspace(
                new AiChatReqVO().setProjectId(99L),
                7L,
                List.of(subAgent),
                value -> value.replace("{projectId}", "99"));

        Path spec = workspace.resolve("subagents").resolve("episode_scene_writer.md");
        String generated = Files.readString(spec, StandardCharsets.UTF_8);
        assertTrue(generated.contains(AgentScopeHarnessWorkspaceService.GENERATED_MARKER));
        assertTrue(generated.contains("get_script_episode"));
        assertTrue(generated.contains("save_script_scene"));
        assertTrue(generated.contains("read_file"));
        assertTrue(generated.contains("Handle project 99."));

        Files.writeString(spec, generated + "\nmanual edit\n", StandardCharsets.UTF_8);

        service.prepareWorkspace(
                new AiChatReqVO().setProjectId(99L),
                7L,
                List.of(subAgent),
                value -> value.replace("{projectId}", "99"));

        String afterRegenerate = Files.readString(spec, StandardCharsets.UTF_8);
        assertTrue(afterRegenerate.contains("manual edit"));
    }

    @Test
    void prepareWorkspaceShouldRestrictGeneratedSubagentToolsInPlanMode() throws Exception {
        AiAgentService aiAgentService = mock(AiAgentService.class);
        AiToolConfigService aiToolConfigService = mock(AiToolConfigService.class);
        AgentScopeHarnessWorkspaceService service =
                newService(aiAgentService, aiToolConfigService);

        when(aiAgentService.getByType("episode_scene_writer"))
                .thenReturn(AiAgentDefinition.builder()
                        .type("episode_scene_writer")
                        .enableTools(1)
                        .build());
        ToolExecutor readTool = mock(ToolExecutor.class);
        when(readTool.getToolName()).thenReturn("get_script_episode");
        when(readTool.isReadOnly()).thenReturn(true);
        ToolExecutor writeTool = mock(ToolExecutor.class);
        when(writeTool.getToolName()).thenReturn("save_script_scene");
        when(writeTool.isReadOnly()).thenReturn(false);
        when(aiToolConfigService.getEnabledToolsByAgent("episode_scene_writer"))
                .thenReturn(List.of(readTool, writeTool));

        AiAgentDefinition.SubAgentToolDef subAgent =
                AiAgentDefinition.SubAgentToolDef.builder()
                        .toolName("episode_scene_writer")
                        .displayName("Episode scene writer")
                        .description("Parse one episode into script scenes.")
                        .refAgentType("episode_scene_writer")
                        .systemPromptOverride("Plan safely.")
                        .build();

        Path workspace = service.prepareWorkspace(
                new AiChatReqVO().setProjectId(99L).setHarnessMode("PLAN"),
                7L,
                List.of(subAgent),
                Function.identity());

        Path spec = workspace.resolve("subagents").resolve("episode_scene_writer.md");
        String generated = Files.readString(spec, StandardCharsets.UTF_8);
        assertTrue(generated.contains("get_script_episode"));
        assertTrue(generated.contains("read_file"));
        assertTrue(generated.contains("memory_search"));
        assertTrue(!generated.contains("save_script_scene"));

        service.prepareWorkspace(
                new AiChatReqVO().setProjectId(99L).setHarnessMode("BUILD"),
                7L,
                List.of(subAgent),
                Function.identity());

        String buildSpec = Files.readString(spec, StandardCharsets.UTF_8);
        assertTrue(buildSpec.contains("save_script_scene"));
    }

    private AgentScopeHarnessWorkspaceService newService() {
        return newService(mock(AiAgentService.class), mock(AiToolConfigService.class));
    }

    private AgentScopeHarnessWorkspaceService newService(
            AiAgentService aiAgentService, AiToolConfigService aiToolConfigService) {
        AgentScopeHarnessProperties properties = new AgentScopeHarnessProperties();
        properties.setWorkspaceRoot(tempDir);
        return new AgentScopeHarnessWorkspaceService(properties, aiAgentService, aiToolConfigService);
    }
}
