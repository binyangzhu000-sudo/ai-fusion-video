package com.stonewu.fusion.service.ai.agentscope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.stonewu.fusion.config.ai.AiAgentDefinition;
import com.stonewu.fusion.controller.ai.vo.AiChatReqVO;
import com.stonewu.fusion.service.ai.AgentConversationService;
import com.stonewu.fusion.service.ai.AgentMessageService;
import com.stonewu.fusion.service.ai.AiAgentService;
import com.stonewu.fusion.service.ai.AiModelService;
import com.stonewu.fusion.service.ai.AiStreamRedisService;
import com.stonewu.fusion.service.ai.AiToolConfigService;
import com.stonewu.fusion.service.ai.ToolExecutor;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

class AgentScopeAssistantServiceToolkitTests {

    @Test
    void toolkitBusinessToolsShouldIncludeSelectedSubAgentRefAgentTools() {
        AiAgentService aiAgentService = mock(AiAgentService.class);
        AiToolConfigService aiToolConfigService = mock(AiToolConfigService.class);
        AgentScopeAssistantService service = newService(aiAgentService, aiToolConfigService);

        when(aiAgentService.getByType("asset_image_gen"))
                .thenReturn(AiAgentDefinition.builder()
                        .type("asset_image_gen")
                        .enableTools(1)
                        .build());

        ToolExecutor parentTool = tool("get_project", true);
        ToolExecutor capabilityTool = tool("get_generation_model_capabilities", true);
        ToolExecutor generateImageTool = tool("generate_image", false);
        ToolExecutor updateImageTool = tool("update_asset_image", false);
        when(aiToolConfigService.getEnabledToolsByAgent("asset_image_gen"))
                .thenReturn(List.of(parentTool));
        when(aiToolConfigService.getEnabledToolsByAgent("asset_image_executor"))
                .thenReturn(List.of(capabilityTool, generateImageTool, updateImageTool));

        AiChatReqVO reqVO = new AiChatReqVO()
                .setAgentType("asset_image_gen")
                .setEnabledTools(List.of("generate_asset_image"));
        AiAgentDefinition.SubAgentToolDef subAgent =
                AiAgentDefinition.SubAgentToolDef.builder()
                        .toolName("generate_asset_image")
                        .refAgentType("asset_image_executor")
                        .build();

        List<String> toolNames = service.resolveToolkitBusinessTools(reqVO, List.of(subAgent))
                .stream()
                .map(ToolExecutor::getToolName)
                .toList();

        assertThat(toolNames)
                .contains("get_generation_model_capabilities", "generate_image", "update_asset_image")
                .doesNotContain("get_project");
    }

    @Test
    void toolkitBusinessToolsShouldRestrictSubAgentRefToolsOnlyInPlanMode() {
        AiAgentService aiAgentService = mock(AiAgentService.class);
        AiToolConfigService aiToolConfigService = mock(AiToolConfigService.class);
        AgentScopeAssistantService service = newService(aiAgentService, aiToolConfigService);

        when(aiAgentService.getByType("asset_image_gen"))
                .thenReturn(AiAgentDefinition.builder()
                        .type("asset_image_gen")
                        .enableTools(1)
                        .build());

        ToolExecutor capabilityTool = tool("get_generation_model_capabilities", true);
        ToolExecutor generateImageTool = tool("generate_image", false);
        when(aiToolConfigService.getEnabledToolsByAgent("asset_image_gen"))
                .thenReturn(List.of());
        when(aiToolConfigService.getEnabledToolsByAgent("asset_image_executor"))
                .thenReturn(List.of(capabilityTool, generateImageTool));

        AiChatReqVO reqVO = new AiChatReqVO()
                .setAgentType("asset_image_gen")
                .setHarnessMode("PLAN");
        AiAgentDefinition.SubAgentToolDef subAgent =
                AiAgentDefinition.SubAgentToolDef.builder()
                        .toolName("generate_asset_image")
                        .refAgentType("asset_image_executor")
                        .build();

        List<String> toolNames = service.resolveToolkitBusinessTools(reqVO, List.of(subAgent))
                .stream()
                .map(ToolExecutor::getToolName)
                .toList();

        assertThat(toolNames)
                .contains("get_generation_model_capabilities")
                .doesNotContain("generate_image");
    }

    private AgentScopeAssistantService newService(
            AiAgentService aiAgentService,
            AiToolConfigService aiToolConfigService) {
        return new AgentScopeAssistantService(
                mock(AiModelService.class),
                aiAgentService,
                aiToolConfigService,
                mock(AgentConversationService.class),
                mock(AgentMessageService.class),
                mock(AgentScopeModelFactory.class),
                mock(AgentScopeHarnessWorkspaceService.class),
                new AgentScopeHarnessProperties(),
                mock(StringRedisTemplate.class),
                mock(AiStreamRedisService.class),
                mock(DataSource.class));
    }

    private ToolExecutor tool(String name, boolean readOnly) {
        ToolExecutor tool = mock(ToolExecutor.class);
        when(tool.getToolName()).thenReturn(name);
        when(tool.isReadOnly()).thenReturn(readOnly);
        return tool;
    }
}
