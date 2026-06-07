"use client";

import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import {
  Bot,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Loader2,
  Wrench,
  XCircle,
} from "lucide-react";
import { StreamMarkdown } from "@/components/dashboard/stream-markdown";
import { StreamThink } from "@/components/dashboard/stream-think";
import { cn } from "@/lib/utils";
import {
  getSubAgentDisplayToolName,
  getToolDisplayName,
  isSubAgentTool,
} from "../shared/ai-task-display";
import { ToolResultDisplay } from "./results";
import type { SubTimelineItem, TimelineItem } from "./types";

/**
 * 剥除所有格式化/标点字符，只留核心语义字符（中日韩字符、字母、数字）。
 * 与后端 AgentScopeEventBridge.isCoreChar 对齐，确保跨 scope 的
 * 文本（主 Agent vs 子 Agent）在格式差异（如 `-` vs `•`、空格差异）下仍可正确匹配。
 */
function normalizeTimelineText(text?: string | null) {
  return (text ?? "")
    .replace(/[\s*_`#>+\-•·●\[\]【】"'\u201c\u201d\u2018\u2019。！？.!?\\:：,，;；()（）]/g, "");
}

/**
 * 计算两段归一化文本的公共前缀占比（相对于较短串）。
 * 用于处理 LLM 输出存在微小差异（如多/少一个字、标点位置不同）的情况。
 */
function textOverlapRatio(a: string, b: string): number {
  if (!a || !b) return 0;
  const shorter = a.length <= b.length ? a : b;
  const longer = a.length > b.length ? a : b;
  if (longer.includes(shorter)) return 1.0;
  let common = 0;
  const maxLen = Math.min(shorter.length, longer.length);
  while (common < maxLen && shorter[common] === longer[common]) {
    common++;
  }
  return common / shorter.length;
}

function isSimilarText(a: string, b: string): boolean {
  if (!a || !b) return false;
  if (a.includes(b) || b.includes(a)) return true;
  return textOverlapRatio(a, b) >= 0.7;
}

function isEquivalentTimelineText(left?: string | null, right?: string | null) {
  const normalizedLeft = normalizeTimelineText(left);
  const normalizedRight = normalizeTimelineText(right);
  if (!normalizedLeft || !normalizedRight) {
    return false;
  }
  return isSimilarText(normalizedLeft, normalizedRight);
}

function isRedundantAfterTool(
  text: string,
  previousTool: Extract<TimelineItem, { type: "tool" }>
) {
  const normalizedText = normalizeTimelineText(text);
  if (!normalizedText || normalizedText.length < 10) {
    return false;
  }
  // 检查是否与工具结果重复
  if (previousTool.result) {
    const nr = normalizeTimelineText(previousTool.result);
    if (nr && isSimilarText(normalizedText, nr)) {
      return true;
    }
  }
  // 检查是否与工具子 Agent 内容重复
  const childText = (previousTool.children ?? [])
    .filter((child): child is Extract<SubTimelineItem, { type: "content" }> => child.type === "content")
    .map((child) => child.text)
    .join("");
  if (childText) {
    const nc = normalizeTimelineText(childText);
    if (nc && isSimilarText(normalizedText, nc)) {
      return true;
    }
  }
  return false;
}

function SubTimelineToolItem({
  child,
}: {
  child: Extract<SubTimelineItem, { type: "tool" }>;
}) {
  const [expanded, setExpanded] = useState(child.status !== "calling");
  const hasResult =
    (child.status === "done" || child.status === "error") && !!child.result;

  return (
    <div
      className={cn(
        "rounded-lg border text-xs overflow-hidden",
        child.status === "calling" && "border-blue-500/20 bg-blue-500/5",
        child.status === "done" && "border-green-500/20 bg-green-500/5",
        child.status === "error" && "border-destructive/20 bg-destructive/5"
      )}
    >
      <div
        className={cn(
          "flex items-center gap-2 px-3 py-2",
          hasResult &&
            "cursor-pointer hover:bg-black/5 dark:hover:bg-white/5 transition-colors"
        )}
        onClick={() => hasResult && setExpanded(!expanded)}
      >
        {child.status === "calling" ? (
          <Loader2 className="h-3 w-3 animate-spin text-blue-400 shrink-0" />
        ) : child.status === "done" ? (
          <CheckCircle2 className="h-3 w-3 text-green-400 shrink-0" />
        ) : (
          <XCircle className="h-3 w-3 text-destructive shrink-0" />
        )}
        {isSubAgentTool(child.name) ? (
          <Bot className="h-3 w-3 text-purple-400 shrink-0" />
        ) : (
          <Wrench className="h-3 w-3 text-muted-foreground shrink-0" />
        )}
        <span className="font-medium min-w-0 truncate">
          {getToolDisplayName(child.name)}
        </span>
        <span className="ml-auto flex items-center gap-1 text-muted-foreground/60 shrink-0">
          {child.status === "calling" ? "执行中..." : child.status === "done" ? "完成" : "失败"}
          {hasResult &&
            (expanded ? (
              <ChevronDown className="h-3 w-3" />
            ) : (
              <ChevronRight className="h-3 w-3" />
            ))}
        </span>
      </div>

      <AnimatePresence initial={false}>
        {hasResult && expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div
              className={cn(
                "border-t px-3 py-2",
                child.status === "error"
                  ? "border-destructive/10"
                  : "border-green-500/10"
              )}
            >
              <ToolResultDisplay toolName={child.name} result={child.result!} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function ToolTimelineItem({
  item,
  isExpanded,
  onToggle,
}: {
  item: Extract<TimelineItem, { type: "tool" }>;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const displayToolName = getSubAgentDisplayToolName(
    item.name,
    item.agentName,
    item.arguments
  );
  const isSubAgentLike =
    !!item.agentName ||
    isSubAgentTool(item.name) ||
    isSubAgentTool(displayToolName) ||
    !!item.children?.length;
  const hasResult =
    (item.status === "done" || item.status === "error") && item.result;
  const hasChildren = !!item.children?.length;
  const lastContentChild = [...(item.children ?? [])]
    .reverse()
    .find(
      (
        child
      ): child is Extract<SubTimelineItem, { type: "content" }> =>
        child.type === "content"
    );
  const renderedResult =
    hasResult && isEquivalentTimelineText(item.result, lastContentChild?.text)
      ? null
      : item.result;

  console.log("=== ToolTimelineItem ===", {
    id: item.id,
    name: item.name,
    displayToolName,
    agentName: item.agentName,
    hasResult,
    hasChildren,
    children: item.children,
    result: item.result,
    renderedResult,
  });

  const canExpand = !!renderedResult || hasChildren;

  return (
    <motion.div
      initial={{ opacity: 0, x: -8 }}
      animate={{ opacity: 1, x: 0 }}
      className={cn(
        "rounded-xl text-sm border overflow-hidden",
        item.status === "calling" &&
          (isSubAgentLike
            ? "border-purple-500/20 bg-purple-500/5"
            : "border-blue-500/20 bg-blue-500/5"),
        item.status === "done" && "border-green-500/20 bg-green-500/5",
        item.status === "error" && "border-destructive/20 bg-destructive/5"
      )}
    >
      <div
        className={cn(
          "flex items-center gap-3 px-4 py-2.5",
          canExpand && "cursor-pointer hover:bg-black/5 dark:hover:bg-white/5 transition-colors"
        )}
        onClick={() => canExpand && onToggle()}
      >
        {item.status === "calling" ? (
          <Loader2
            className={cn(
              "h-3.5 w-3.5 animate-spin shrink-0",
              isSubAgentLike ? "text-purple-400" : "text-blue-400"
            )}
          />
        ) : item.status === "done" ? (
          <CheckCircle2 className="h-3.5 w-3.5 text-green-400 shrink-0" />
        ) : (
          <XCircle className="h-3.5 w-3.5 text-destructive shrink-0" />
        )}
        {item.agentName || isSubAgentTool(item.name) ? (
          <Bot className="h-3.5 w-3.5 text-purple-400 shrink-0" />
        ) : (
          <Wrench className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
        )}
        <span className="font-medium text-xs min-w-0 truncate">
          {getToolDisplayName(displayToolName)}
        </span>
        {item.agentName && item.agentName !== displayToolName && (
          <span className="text-[10px] text-muted-foreground/60 truncate max-w-[160px]">
            {item.agentName}
          </span>
        )}
        {item.status === "calling" && (
          <span
            className={cn(
              "text-xs ml-auto shrink-0",
              isSubAgentLike
                ? "text-purple-400/80"
                : "text-muted-foreground"
            )}
          >
            {isSubAgentLike ? "运行中..." : "执行中..."}
          </span>
        )}
        {item.status === "done" && (
          <span className="flex items-center gap-1.5 text-xs text-green-400/80 ml-auto">
            ✓ 完成
            {canExpand &&
              (isExpanded ? (
                <ChevronDown className="h-3 w-3" />
              ) : (
                <ChevronRight className="h-3 w-3" />
              ))}
          </span>
        )}
        {item.status === "error" && (
          <span className="flex items-center gap-1.5 text-xs text-destructive ml-auto">
            ✗ 失败
            {canExpand &&
              (isExpanded ? (
                <ChevronDown className="h-3 w-3" />
              ) : (
                <ChevronRight className="h-3 w-3" />
              ))}
          </span>
        )}
      </div>

      <AnimatePresence>
        {isExpanded && (renderedResult || hasChildren) && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div
              className={cn(
                "border-t px-4 py-3 space-y-2",
                item.status === "error"
                  ? "border-destructive/10"
                  : "border-green-500/10"
              )}
            >
              {item.children && item.children.length > 0 && (
                <div className="space-y-2 pl-2 border-l-2 border-purple-500/20">
                  {item.children.map((child, childIndex) => {
                    const childCount = item.children?.length ?? 0;
                    if (child.type === "reasoning") {
                      const childIsStreaming =
                        item.status === "calling" && childIndex === childCount - 1;
                      const childTitle = child.durationMs
                        ? `子Agent 思考 (${(child.durationMs / 1000).toFixed(1)}s)`
                        : childIsStreaming
                          ? "子Agent 思考中"
                          : "子Agent 思考";
                      return (
                        <div key={`sub-reasoning-${childIndex}`} className="text-xs">
                          <StreamThink
                            title={childTitle}
                            content={child.text}
                            compact
                            maxHeight={120}
                            streaming={childIsStreaming}
                          />
                        </div>
                      );
                    }

                    if (child.type === "tool") {
                      return (
                        <SubTimelineToolItem
                          key={`sub-tool-${child.id}`}
                          child={child}
                        />
                      );
                    }

                    const trimmedText = child.text.trim();
                    const looksLikeJson =
                      trimmedText.length > 50 &&
                      ((trimmedText.startsWith("{") && trimmedText.endsWith("}")) ||
                        (trimmedText.startsWith("[") && trimmedText.endsWith("]")));
                    
                    if (looksLikeJson) {
                      return null;
                    }

                    const normalizedChildText = normalizeTimelineText(child.text);
                    if (normalizedChildText && normalizedChildText.length >= 10) {
                      const isRedundant = item.children.some((other) => {
                        if (other === child) return false;
                        if (other.type === "tool" && other.result) {
                          const nr = normalizeTimelineText(other.result);
                          if (nr && isSimilarText(normalizedChildText, nr)) return true;
                        }
                        // 与同级其他 content 子项比较，只跳过比自身短的
                        if (other.type === "content" && other !== child) {
                          const nc = normalizeTimelineText(other.text);
                          if (nc && nc.length > normalizedChildText.length && nc.includes(normalizedChildText)) return true;
                        }
                        return false;
                      });
                      if (isRedundant) {
                        return null;
                      }
                    }

                    return (
                      <div
                        key={`sub-content-${childIndex}`}
                        className="rounded-lg border border-border/20 bg-card/20 p-3 text-xs leading-relaxed"
                      >
                        <StreamMarkdown content={child.text} compact />
                      </div>
                    );
                  })}
                </div>
              )}
              {renderedResult && (
                <ToolResultDisplay toolName={displayToolName} result={renderedResult} />
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

export function AgentPipelineTimeline({
  timeline,
  isActive,
}: {
  timeline: TimelineItem[];
  isActive: boolean;
}) {
  const [expandedTools, setExpandedTools] = useState<Set<string>>(new Set());
  const [collapsedSubAgentTools, setCollapsedSubAgentTools] = useState<Set<string>>(
    new Set()
  );
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [timeline]);

  if (timeline.length === 0) {
    return null;
  }

  return (
    <div ref={scrollRef} className="space-y-2 max-h-[60vh] overflow-y-auto">
      {timeline.map((item, index) => {
        if (item.type === "reasoning") {
          const title = item.durationMs
            ? `思考 (${(item.durationMs / 1000).toFixed(1)}s)`
            : isActive && index === timeline.length - 1
              ? "思考中"
              : "思考";
          return (
            <motion.div
              key={`reasoning-${index}`}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
            >
              <StreamThink
                title={title}
                content={item.text}
                streaming={isActive && index === timeline.length - 1}
              />
            </motion.div>
          );
        }

        if (item.type === "tool") {
          const displayToolName = getSubAgentDisplayToolName(
            item.name,
            item.agentName,
            item.arguments
          );
          const isSubAgentLike =
            !!item.agentName ||
            isSubAgentTool(item.name) ||
            isSubAgentTool(displayToolName) ||
            !!item.children?.length;
          const isExpanded = isSubAgentLike
            ? !collapsedSubAgentTools.has(item.id)
            : expandedTools.has(item.id);
          return (
            <ToolTimelineItem
              key={`tool-${item.id}`}
              item={item}
              isExpanded={isExpanded}
              onToggle={() => {
                if (isSubAgentLike) {
                  setCollapsedSubAgentTools((prev) => {
                    const next = new Set(prev);
                    if (next.has(item.id)) {
                      next.delete(item.id);
                    } else {
                      next.add(item.id);
                    }
                    return next;
                  });
                } else {
                  setExpandedTools((prev) => {
                    const next = new Set(prev);
                    if (next.has(item.id)) {
                      next.delete(item.id);
                    } else {
                      next.add(item.id);
                    }
                    return next;
                  });
                }
              }}
            />
          );
        }

        const trimmedText = item.text.trim();
        const looksLikeJson =
          trimmedText.length > 50 &&
          ((trimmedText.startsWith("{") && trimmedText.endsWith("}")) ||
            (trimmedText.startsWith("[") && trimmedText.endsWith("]")));
        if (looksLikeJson) {
          return null;
        }

        const hasToolsInTimeline = timeline.some((t) => t.type === "tool");
        if (hasToolsInTimeline) {
          // 与任何 tool 的 result 内容重复
          const isRedundant = timeline.some(
            (t) =>
              t.type === "tool" &&
              (t.children?.length || t.result) &&
              isRedundantAfterTool(item.text, t)
          );
          if (isRedundant) {
            return null;
          }
        }

        return (
          <motion.div
            key={`content-${index}`}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className={cn(
              "rounded-xl border border-border/30 bg-card/30 p-4",
              "text-sm leading-relaxed"
            )}
          >
            <StreamMarkdown
              content={item.text}
              streaming={isActive && index === timeline.length - 1}
            />
          </motion.div>
        );
      })}
    </div>
  );
}
