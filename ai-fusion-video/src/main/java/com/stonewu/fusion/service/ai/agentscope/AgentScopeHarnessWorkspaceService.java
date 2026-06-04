package com.stonewu.fusion.service.ai.agentscope;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.stonewu.fusion.config.ai.AiAgentDefinition;
import com.stonewu.fusion.controller.ai.vo.AiChatReqVO;
import com.stonewu.fusion.service.ai.AiAgentService;
import com.stonewu.fusion.service.ai.AiToolConfigService;
import com.stonewu.fusion.service.ai.ToolExecutor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AgentScopeHarnessWorkspaceService {

    static final String GENERATED_MARKER =
            "<!-- generated-by: ai-fusion-video-harness -->";
    private static final Pattern CHECKSUM_PATTERN =
            Pattern.compile("(?m)^<!-- generated-checksum: ([0-9a-f]+) -->\\R?");
    private static final List<String> WORKSPACE_DIRECTORIES =
            List.of("skills", "skills/_drafts", "subagents", "plans", "memory", "knowledge");
    private static final String NO_INHERITED_TOOL = "__none__";

    private final AgentScopeHarnessProperties properties;
    private final AiAgentService aiAgentService;
    private final AiToolConfigService aiToolConfigService;

    public Path prepareWorkspace(
            AiChatReqVO reqVO,
            Long userId,
            List<AiAgentDefinition.SubAgentToolDef> subAgentTools,
            Function<String, String> templateResolver) {
        Path workspace = resolveWorkspacePath(reqVO != null ? reqVO.getProjectId() : null, userId);
        initializeWorkspace(workspace);
        generateSubagentSpecs(
                workspace,
                subAgentTools,
                templateResolver != null ? templateResolver : Function.identity(),
                shouldUseReadOnlySubagentTools(reqVO));
        return workspace;
    }

    public Path resolveWorkspacePath(Long projectId, Long userId) {
        Path root = properties.getWorkspaceRoot().toAbsolutePath().normalize();
        if (projectId != null && projectId > 0) {
            return root.resolve("projects").resolve(sanitizePathSegment(projectId.toString()));
        }
        String userSegment = userId != null ? userId.toString() : "anonymous";
        return root.resolve("users").resolve(sanitizePathSegment(userSegment));
    }

    void initializeWorkspace(Path workspace) {
        try {
            Files.createDirectories(workspace);
            for (String directory : WORKSPACE_DIRECTORIES) {
                Files.createDirectories(workspace.resolve(directory));
            }
            writeIfMissing(
                    workspace.resolve("AGENTS.md"),
                    """
                    # AgentScope Harness Workspace

                    This workspace stores persistent instructions, memory, skills, subagent
                    declarations, plans, session logs, and background task records for
                    ai-fusion-video.
                    """);
            writeIfMissing(
                    workspace.resolve("MEMORY.md"),
                    """
                    # Memory

                    Persistent facts and decisions consolidated by HarnessAgent.
                    """);
            writeIfMissing(
                    workspace.resolve("knowledge").resolve("KNOWLEDGE.md"),
                    """
                    # Knowledge

                    Project and user knowledge shared with HarnessAgent.
                    """);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize AgentScope workspace: " + workspace, e);
        }
    }

    private void generateSubagentSpecs(
            Path workspace,
            List<AiAgentDefinition.SubAgentToolDef> subAgentTools,
            Function<String, String> templateResolver,
            boolean readOnlyOnly) {
        if (CollUtil.isEmpty(subAgentTools)) {
            return;
        }
        Path subagentsDir = workspace.resolve("subagents");
        for (AiAgentDefinition.SubAgentToolDef subAgentTool : subAgentTools) {
            if (subAgentTool == null || StrUtil.isBlank(subAgentTool.getToolName())) {
                continue;
            }
            String agentId = sanitizePathSegment(subAgentTool.getToolName());
            Path target = subagentsDir.resolve(agentId + ".md");
            String spec = withChecksum(renderSubagentSpec(subAgentTool, templateResolver, readOnlyOnly));
            writeGeneratedFile(target, spec);
        }
    }

    private boolean shouldUseReadOnlySubagentTools(AiChatReqVO reqVO) {
        if (!properties.isPlanModeEnabled() || !properties.isPlanModeReadOnlySubagentToolsOnly()) {
            return false;
        }
        String mode = reqVO != null ? reqVO.getHarnessMode() : null;
        return "PLAN".equals(StrUtil.blankToDefault(mode, "").trim().toUpperCase(Locale.ROOT));
    }

    private String renderSubagentSpec(
            AiAgentDefinition.SubAgentToolDef subAgentTool,
            Function<String, String> templateResolver,
            boolean readOnlyOnly) {
        String agentId = sanitizePathSegment(subAgentTool.getToolName());
        String displayName = StrUtil.blankToDefault(subAgentTool.getDisplayName(), agentId);
        String description = StrUtil.blankToDefault(subAgentTool.getDescription(), displayName);
        List<String> allowedTools =
                resolveInheritedToolAllowlist(subAgentTool.getRefAgentType(), readOnlyOnly);
        String systemPrompt = resolveSubagentPrompt(subAgentTool, templateResolver);

        StringBuilder spec = new StringBuilder();
        spec.append("---\n");
        spec.append("description: |-\n");
        spec.append(indentYamlBlock(description, 2));
        spec.append("workspace:\n");
        spec.append("  mode: isolated\n");
        spec.append("steps: 999\n");
        spec.append("tools: [").append(String.join(", ", allowedTools)).append("]\n");
        spec.append("---\n\n");
        spec.append(GENERATED_MARKER).append("\n\n");
        spec.append("# ").append(displayName).append("\n\n");
        spec.append(systemPrompt).append("\n\n");
        spec.append("## Invocation Contract\n\n");
        spec.append("This subagent was migrated from the legacy tool `")
                .append(subAgentTool.getToolName())
                .append("`. The main agent should delegate by calling ")
                .append("`agent_spawn(agent_id=\"")
                .append(agentId)
                .append("\", task=...)` or `agent_send` for follow-up work.\n\n");
        appendSchema(spec, "Input JSON Schema", subAgentTool.getParametersSchema());
        appendSchema(spec, "Output JSON Schema", subAgentTool.getOutputSchema());
        return spec.toString();
    }

    private List<String> resolveInheritedToolAllowlist(String refAgentType, boolean readOnlyOnly) {
        Set<String> toolNames = new LinkedHashSet<>();
        AiAgentDefinition agentDef =
                StrUtil.isNotBlank(refAgentType) ? aiAgentService.getByType(refAgentType) : null;
        if (agentDef != null && Integer.valueOf(1).equals(agentDef.getEnableTools())) {
            aiToolConfigService.getEnabledToolsByAgent(refAgentType).stream()
                    .filter(tool -> !readOnlyOnly || tool.isReadOnly())
                    .map(ToolExecutor::getToolName)
                    .filter(StrUtil::isNotBlank)
                    .forEach(toolNames::add);
        }
        if (properties.isReadOnlyWorkspaceToolsEnabled()) {
            toolNames.addAll(AgentScopeReadOnlyWorkspaceTools.TOOL_NAMES);
        }
        return toolNames.isEmpty() ? List.of(NO_INHERITED_TOOL) : new ArrayList<>(toolNames);
    }

    private String resolveSubagentPrompt(
            AiAgentDefinition.SubAgentToolDef subAgentTool,
            Function<String, String> templateResolver) {
        String systemPrompt = subAgentTool.getSystemPromptOverride();
        AiAgentDefinition refDef =
                StrUtil.isNotBlank(subAgentTool.getRefAgentType())
                        ? aiAgentService.getByType(subAgentTool.getRefAgentType())
                        : null;
        if (StrUtil.isBlank(systemPrompt) && refDef != null) {
            systemPrompt = refDef.getSystemPrompt();
        }
        if (StrUtil.isBlank(systemPrompt)) {
            systemPrompt = "You are a task-focused AI subagent.";
        }
        systemPrompt = templateResolver.apply(systemPrompt);

        String instruction = subAgentTool.getInstructionOverride();
        if (StrUtil.isBlank(instruction) && refDef != null) {
            instruction = refDef.getInstructionTemplate();
        }
        if (StrUtil.isNotBlank(instruction)) {
            systemPrompt = systemPrompt + "\n\n" + templateResolver.apply(instruction);
        }
        return systemPrompt.strip();
    }

    private void appendSchema(StringBuilder spec, String title, String schema) {
        if (StrUtil.isBlank(schema)) {
            return;
        }
        spec.append("## ").append(title).append("\n\n");
        spec.append("```json\n").append(schema.strip()).append("\n```\n\n");
    }

    private void writeGeneratedFile(Path target, String content) {
        try {
            if (Files.exists(target)) {
                String existing = Files.readString(target, StandardCharsets.UTF_8);
                if (!isUnmodifiedGeneratedFile(existing)) {
                    log.info("Skip user-modified Harness subagent spec: {}", target);
                    return;
                }
            }
            Files.writeString(
                    target,
                    content,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write Harness subagent spec: " + target, e);
        }
    }

    private boolean isUnmodifiedGeneratedFile(String content) {
        if (content == null || !content.contains(GENERATED_MARKER)) {
            return false;
        }
        Matcher matcher = CHECKSUM_PATTERN.matcher(content);
        if (!matcher.find()) {
            return false;
        }
        String storedChecksum = matcher.group(1);
        return storedChecksum.equals(sha256(stripChecksumLine(content)));
    }

    private String withChecksum(String contentWithoutChecksum) {
        String checksum = sha256(contentWithoutChecksum);
        return contentWithoutChecksum.replace(
                GENERATED_MARKER, GENERATED_MARKER + "\n<!-- generated-checksum: " + checksum + " -->");
    }

    private String stripChecksumLine(String content) {
        return CHECKSUM_PATTERN.matcher(content).replaceFirst("");
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private void writeIfMissing(Path path, String content) throws IOException {
        if (Files.exists(path)) {
            return;
        }
        Files.createDirectories(path.getParent());
        Files.writeString(path, content.strip() + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
    }

    private String indentYamlBlock(String text, int spaces) {
        String indent = " ".repeat(spaces);
        String source = StrUtil.blankToDefault(text, "");
        StringBuilder sb = new StringBuilder();
        for (String line : source.split("\\R", -1)) {
            sb.append(indent).append(line).append('\n');
        }
        return sb.toString();
    }

    private String sanitizePathSegment(String value) {
        String safe = StrUtil.blankToDefault(value, "agent").replaceAll("[^A-Za-z0-9_.-]", "_");
        return StrUtil.isBlank(safe) ? "agent" : safe;
    }
}
