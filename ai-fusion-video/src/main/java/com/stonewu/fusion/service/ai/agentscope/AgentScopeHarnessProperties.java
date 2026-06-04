package com.stonewu.fusion.service.ai.agentscope;

import java.nio.file.Path;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "fusion.agentscope.harness")
public class AgentScopeHarnessProperties {

    private Path workspaceRoot = Path.of("./data/agentscope/workspaces");
    private boolean skillAutoPromote = true;
    private boolean skillSecurityScan = true;
    private boolean skillCuratorEnabled = true;
    private boolean planModeEnabled = true;
    private boolean planModeReadOnlySubagentToolsOnly = true;
    private boolean readOnlyWorkspaceToolsEnabled = true;
    private int maxContextTokens = 8000;
    private Compaction compaction = new Compaction();
    private ToolResultEviction toolResultEviction = new ToolResultEviction();

    @Data
    public static class Compaction {
        private boolean enabled = true;
        private int triggerMessages = 50;
        private int triggerTokens = 80_000;
        private int keepMessages = 20;
        private int keepTokens = 0;
        private boolean flushBeforeCompact = true;
        private boolean offloadBeforeCompact = true;
        private TruncateArgs truncateArgs = new TruncateArgs();
    }

    @Data
    public static class TruncateArgs {
        private boolean enabled = true;
        private int triggerMessages = 25;
        private int triggerTokens = 40_000;
        private int keepMessages = 20;
        private int keepTokens = 0;
        private int maxArgLength = 2000;
    }

    @Data
    public static class ToolResultEviction {
        private boolean enabled = true;
        private int maxResultChars = 80_000;
        private int previewChars = 2000;
        private String evictionPath = "/large_tool_results";
    }
}
