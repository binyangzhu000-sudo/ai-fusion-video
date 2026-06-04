package com.stonewu.fusion.service.ai.agentscope;

import cn.hutool.core.util.StrUtil;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.harness.agent.filesystem.AbstractFilesystem;
import io.agentscope.harness.agent.filesystem.model.FileInfo;
import io.agentscope.harness.agent.filesystem.model.GlobResult;
import io.agentscope.harness.agent.filesystem.model.GrepMatch;
import io.agentscope.harness.agent.filesystem.model.GrepResult;
import io.agentscope.harness.agent.filesystem.model.LsResult;
import io.agentscope.harness.agent.filesystem.model.ReadResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Read-only replacements for Harness RC1 workspace tools.
 *
 * <p>RC1's built-in filesystem and memory tools are annotation-based and are not marked
 * read-only, so Plan Mode blocks even reads. Registering these wrappers keeps reads available
 * while disabling model-facing write/edit filesystem tools.
 */
public class AgentScopeReadOnlyWorkspaceTools {

    public static final List<String> TOOL_NAMES = List.of(
            "read_file",
            "grep_files",
            "glob_files",
            "list_files",
            "memory_search",
            "memory_get",
            "session_search",
            "session_list",
            "session_history");

    private final AbstractFilesystem filesystem;
    private final AbstractFilesystem fallbackFilesystem;

    public AgentScopeReadOnlyWorkspaceTools(AbstractFilesystem filesystem) {
        this(filesystem, null);
    }

    public AgentScopeReadOnlyWorkspaceTools(AbstractFilesystem filesystem, AbstractFilesystem fallbackFilesystem) {
        this.filesystem = filesystem;
        this.fallbackFilesystem = fallbackFilesystem;
    }

    @Tool(
            name = "read_file",
            readOnly = true,
            description =
                    "Read workspace file content with line numbers. Supports pagination via offset and limit.")
    public String readFile(
            RuntimeContext runtimeContext,
            @ToolParam(name = "path", description = "Workspace file path to read") String path,
            @ToolParam(
                            name = "offset",
                            description = "Start line, 0-indexed. Default: 0",
                            required = false)
                    Integer offset,
            @ToolParam(
                            name = "limit",
                            description = "Max lines to return. Default: 0 means all lines",
                            required = false)
                    Integer limit) {
        ReadResult result = readFirstAvailable(
                ctx(runtimeContext), path, offset != null ? offset : 0, limit != null ? limit : 0);
        if (!result.isSuccess()) {
            return "Error: " + result.error();
        }
        return result.fileData() != null ? result.fileData().content() : "";
    }

    @Tool(
            name = "grep_files",
            readOnly = true,
            description = "Search workspace file contents for a literal text pattern.")
    public String grepFiles(
            RuntimeContext runtimeContext,
            @ToolParam(name = "pattern", description = "Literal text pattern to search for")
                    String pattern,
            @ToolParam(
                            name = "path",
                            description = "Directory or file to search. Default: workspace root",
                            required = false)
                    String path,
            @ToolParam(
                            name = "glob",
                            description = "Optional file glob filter, for example *.java",
                            required = false)
                    String glob) {
        List<GrepMatch> matches = grepAll(ctx(runtimeContext), pattern, StrUtil.blankToDefault(path, "."), glob);
        if (matches == null || matches.isEmpty()) {
            return "No matches found";
        }
        return matches.stream()
                .map(match -> match.path() + ":" + match.line() + ":" + match.text())
                .collect(Collectors.joining("\n"));
    }

    @Tool(
            name = "glob_files",
            readOnly = true,
            description = "Find workspace files matching a glob pattern.")
    public String globFiles(
            RuntimeContext runtimeContext,
            @ToolParam(name = "pattern", description = "Glob pattern, for example **/*.java")
                    String pattern,
            @ToolParam(
                            name = "path",
                            description = "Base directory to search from. Default: workspace root",
                            required = false)
                    String path) {
        List<FileInfo> files = globAll(ctx(runtimeContext), pattern, StrUtil.blankToDefault(path, "."));
        if (files == null || files.isEmpty()) {
            return "No matching files found";
        }
        return files.stream()
                .map(file -> file.path() + (file.isDirectory() ? "/" : " (" + file.size() + " bytes)"))
                .collect(Collectors.joining("\n"));
    }

    @Tool(
            name = "list_files",
            readOnly = true,
            description = "List workspace files and directories at the given path.")
    public String listFiles(
            RuntimeContext runtimeContext,
            @ToolParam(
                            name = "path",
                            description = "Directory path to list. Default: workspace root",
                            required = false)
                    String path) {
        List<FileInfo> entries = lsAll(ctx(runtimeContext), StrUtil.blankToDefault(path, "."));
        if (entries == null || entries.isEmpty()) {
            return "Empty or not a directory: " + StrUtil.blankToDefault(path, ".");
        }
        return entries.stream()
                .map(file -> (file.isDirectory() ? "[DIR]  " : "[FILE] ")
                        + file.path()
                        + (file.isDirectory() ? "" : " (" + file.size() + " bytes)"))
                .collect(Collectors.joining("\n"));
    }

    @Tool(
            name = "memory_search",
            readOnly = true,
            description =
                    "Search long-term memory files, including MEMORY.md and memory/*.md, for relevant facts.")
    public String memorySearch(
            RuntimeContext runtimeContext,
            @ToolParam(name = "query", description = "Keywords to search for in memory files")
                    String query) {
        if (StrUtil.isBlank(query)) {
            return "No query provided";
        }

        RuntimeContext ctx = ctx(runtimeContext);
        Pattern pattern = Pattern.compile(Pattern.quote(query), Pattern.CASE_INSENSITIVE);
        StringJoiner results = new StringJoiner("\n");
        int matchCount = 0;

        for (String relativePath : memoryPaths(ctx)) {
            ReadResult read = readFirstAvailable(ctx, relativePath, 0, 0);
            if (!read.isSuccess() || read.fileData() == null || read.fileData().content() == null) {
                continue;
            }
            String[] lines = read.fileData().content().split("\n", -1);
            for (int i = 0; i < lines.length; i++) {
                if (pattern.matcher(lines[i]).find()) {
                    results.add(String.format("Source: %s#%d: %s", relativePath, i + 1, lines[i]));
                    matchCount++;
                }
            }
        }

        if (matchCount == 0) {
            return "No matching memories found for: " + query;
        }
        return "Found " + matchCount + " matches:\n\n" + results;
    }

    @Tool(
            name = "memory_get",
            readOnly = true,
            description =
                    "Read specific lines from MEMORY.md or memory/*.md. Use after memory_search for surrounding context.")
    public String memoryGet(
            RuntimeContext runtimeContext,
            @ToolParam(
                            name = "path",
                            description = "Memory path, for example MEMORY.md or memory/2026-04-01.md")
                    String path,
            @ToolParam(name = "startLine", description = "Start line number, 1-based and inclusive")
                    Integer startLine,
            @ToolParam(name = "endLine", description = "End line number, 1-based and inclusive")
                    Integer endLine) {
        String memoryPath = normalizeMemoryPath(path);
        if (memoryPath == null) {
            return "Error: path must be MEMORY.md or under memory/";
        }

        ReadResult read = readFirstAvailable(ctx(runtimeContext), memoryPath, 0, 0);
        if (!read.isSuccess() || read.fileData() == null || StrUtil.isBlank(read.fileData().content())) {
            return "Error: file not found: " + memoryPath;
        }

        List<String> lines = List.of(read.fileData().content().split("\n", -1));
        int start = Math.max(0, (startLine != null ? startLine : 1) - 1);
        int end = Math.min(lines.size(), endLine != null && endLine > 0 ? endLine : lines.size());
        if (start >= lines.size()) {
            return "Error: startLine " + (startLine != null ? startLine : 1)
                    + " exceeds file length " + lines.size();
        }

        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(String.format("%d|%s%n", i + 1, lines.get(i)));
        }
        return sb.toString();
    }

    @Tool(
            name = "session_search",
            readOnly = true,
            description =
                    "Search uncompressed session log files for a keyword or phrase.")
    public String sessionSearch(
            RuntimeContext runtimeContext,
            @ToolParam(name = "query", description = "Search query") String query,
            @ToolParam(
                            name = "agentId",
                            description = "Agent ID to search. Omit to search all visible agents",
                            required = false)
                    String agentId,
            @ToolParam(
                            name = "maxResults",
                            description = "Maximum number of matching log lines to return. Default: 10",
                            required = false)
                    Integer maxResults) {
        if (StrUtil.isBlank(query)) {
            return "Error: query is required";
        }
        RuntimeContext ctx = ctx(runtimeContext);
        int limit = maxResults != null && maxResults > 0 ? maxResults : 10;
        Pattern pattern = Pattern.compile(Pattern.quote(query), Pattern.CASE_INSENSITIVE);
        List<String> results = new ArrayList<>();

        for (String logPath : sessionLogPaths(ctx, agentId)) {
            if (results.size() >= limit) {
                break;
            }
            ReadResult read = readFirstAvailable(ctx, logPath, 0, 0);
            if (!read.isSuccess() || read.fileData() == null || read.fileData().content() == null) {
                continue;
            }
            String[] lines = read.fileData().content().split("\n", -1);
            for (int i = 0; i < lines.length && results.size() < limit; i++) {
                if (pattern.matcher(lines[i]).find()) {
                    results.add(logPath + "#" + (i + 1) + ": " + truncate(lines[i], 500));
                }
            }
        }

        if (results.isEmpty()) {
            return "No matches found for: " + query;
        }
        return "Found " + results.size() + " matches for \"" + query + "\":\n\n"
                + String.join("\n", results);
    }

    @Tool(
            name = "session_list",
            readOnly = true,
            description = "List available uncompressed sessions for an agent.")
    public String sessionList(
            RuntimeContext runtimeContext,
            @ToolParam(name = "agentId", description = "Agent ID to list sessions for")
                    String agentId) {
        if (StrUtil.isBlank(agentId)) {
            return "Error: agentId is required";
        }
        RuntimeContext ctx = ctx(runtimeContext);

        ReadResult sessionStore = readFirstAvailable(ctx, "agents/" + agentId + "/sessions/sessions.json", 0, 0);
        if (sessionStore.isSuccess()
                && sessionStore.fileData() != null
                && StrUtil.isNotBlank(sessionStore.fileData().content())) {
            return sessionStore.fileData().content();
        }

        List<String> logPaths = sessionLogPaths(ctx, agentId);
        if (logPaths.isEmpty()) {
            return "No sessions found for agent: " + agentId;
        }
        return "Sessions for agent " + agentId + ":\n"
                + logPaths.stream()
                        .map(this::sessionIdFromLogPath)
                        .distinct()
                        .map(sessionId -> "  - " + sessionId)
                        .collect(Collectors.joining("\n"));
    }

    @Tool(
            name = "session_history",
            readOnly = true,
            description = "Read recent raw lines from an uncompressed session log.")
    public String sessionHistory(
            RuntimeContext runtimeContext,
            @ToolParam(name = "agentId", description = "Agent ID") String agentId,
            @ToolParam(name = "sessionId", description = "Session ID") String sessionId,
            @ToolParam(
                            name = "lastN",
                            description = "Number of recent log lines to return. Default: 20",
                            required = false)
                    Integer lastN) {
        if (StrUtil.isBlank(agentId) || StrUtil.isBlank(sessionId)) {
            return "Error: agentId and sessionId are required";
        }
        String logPath = "agents/" + agentId + "/sessions/" + sessionId + ".log.jsonl";
        ReadResult read = readFirstAvailable(ctx(runtimeContext), logPath, 0, 0);
        if (!read.isSuccess() || read.fileData() == null || StrUtil.isBlank(read.fileData().content())) {
            return "Session not found: " + sessionId;
        }

        List<String> lines = List.of(read.fileData().content().split("\n", -1));
        int limit = lastN != null && lastN > 0 ? lastN : 20;
        int start = Math.max(0, lines.size() - limit);
        StringBuilder sb = new StringBuilder();
        sb.append("Session ").append(sessionId)
                .append(" (").append(lines.size()).append(" total log lines, showing last ")
                .append(Math.min(limit, lines.size())).append("):\n\n");
        for (int i = start; i < lines.size(); i++) {
            if (StrUtil.isNotBlank(lines.get(i))) {
                sb.append(i + 1).append("|").append(truncate(lines.get(i), 1000)).append('\n');
            }
        }
        return sb.toString();
    }

    private List<String> memoryPaths(RuntimeContext runtimeContext) {
        Set<String> paths = new LinkedHashSet<>();
        if (readFirstAvailable(runtimeContext, "MEMORY.md", 0, 0).isSuccess()) {
            paths.add("MEMORY.md");
        }
        for (FileInfo file : globAll(runtimeContext, "*.md", "memory")) {
            if (!file.isDirectory()) {
                paths.add(file.path());
            }
        }
        return new ArrayList<>(paths);
    }

    private List<String> sessionLogPaths(RuntimeContext runtimeContext, String agentId) {
        String basePath = StrUtil.isNotBlank(agentId) ? "agents/" + agentId + "/sessions" : "agents";
        return globAll(runtimeContext, "*.log.jsonl", basePath).stream()
                .filter(file -> !file.isDirectory())
                .map(FileInfo::path)
                .sorted()
                .toList();
    }

    private String sessionIdFromLogPath(String path) {
        if (path == null) {
            return "";
        }
        int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        String fileName = slash >= 0 ? path.substring(slash + 1) : path;
        return fileName.endsWith(".log.jsonl")
                ? fileName.substring(0, fileName.length() - ".log.jsonl".length())
                : fileName;
    }

    private String normalizeMemoryPath(String path) {
        if (StrUtil.isBlank(path)) {
            return null;
        }
        String cleaned = path.replace('\\', '/').replaceFirst("^/+", "");
        Path normalized = Path.of(cleaned).normalize();
        String relative = normalized.toString().replace('\\', '/');
        if ("MEMORY.md".equals(relative) || relative.startsWith("memory/")) {
            return relative;
        }
        return null;
    }

    private RuntimeContext ctx(RuntimeContext runtimeContext) {
        return runtimeContext != null ? runtimeContext : RuntimeContext.empty();
    }

    private ReadResult readFirstAvailable(RuntimeContext ctx, String path, int offset, int limit) {
        ReadResult primary = filesystem.read(ctx, path, offset, limit);
        if (hasContent(primary) || fallbackFilesystem == null) {
            return primary;
        }
        ReadResult fallback = fallbackFilesystem.read(ctx, path, offset, limit);
        return hasContent(fallback) ? fallback : primary;
    }

    private boolean hasContent(ReadResult result) {
        return result != null
                && result.isSuccess()
                && result.fileData() != null
                && StrUtil.isNotEmpty(result.fileData().content());
    }

    private List<FileInfo> globAll(RuntimeContext ctx, String pattern, String path) {
        Map<String, FileInfo> files = new LinkedHashMap<>();
        collectGlob(files, filesystem, ctx, pattern, path);
        collectGlob(files, fallbackFilesystem, ctx, pattern, path);
        return new ArrayList<>(files.values());
    }

    private void collectGlob(Map<String, FileInfo> files, AbstractFilesystem fs,
            RuntimeContext ctx, String pattern, String path) {
        if (fs == null) {
            return;
        }
        GlobResult result = fs.glob(ctx, pattern, path);
        if (!result.isSuccess() || result.matches() == null) {
            return;
        }
        for (FileInfo file : result.matches()) {
            files.putIfAbsent(file.path(), file);
        }
    }

    private List<FileInfo> lsAll(RuntimeContext ctx, String path) {
        Map<String, FileInfo> entries = new LinkedHashMap<>();
        collectLs(entries, filesystem, ctx, path);
        collectLs(entries, fallbackFilesystem, ctx, path);
        return new ArrayList<>(entries.values());
    }

    private void collectLs(Map<String, FileInfo> entries, AbstractFilesystem fs, RuntimeContext ctx, String path) {
        if (fs == null) {
            return;
        }
        LsResult result = fs.ls(ctx, path);
        if (!result.isSuccess() || result.entries() == null) {
            return;
        }
        for (FileInfo entry : result.entries()) {
            entries.putIfAbsent(entry.path(), entry);
        }
    }

    private List<GrepMatch> grepAll(RuntimeContext ctx, String pattern, String path, String glob) {
        List<GrepMatch> matches = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        collectGrep(matches, seen, filesystem, ctx, pattern, path, glob);
        collectGrep(matches, seen, fallbackFilesystem, ctx, pattern, path, glob);
        return matches;
    }

    private void collectGrep(List<GrepMatch> matches, Set<String> seen, AbstractFilesystem fs,
            RuntimeContext ctx, String pattern, String path, String glob) {
        if (fs == null) {
            return;
        }
        GrepResult result = fs.grep(ctx, pattern, path, glob);
        if (!result.isSuccess() || result.matches() == null) {
            return;
        }
        for (GrepMatch match : result.matches()) {
            String key = match.path() + ":" + match.line() + ":" + match.text();
            if (seen.add(key)) {
                matches.add(match);
            }
        }
    }

    private String truncate(String text, int maxChars) {
        if (text == null || text.length() <= maxChars) {
            return text;
        }
        return text.substring(0, maxChars) + "... [truncated]";
    }
}
