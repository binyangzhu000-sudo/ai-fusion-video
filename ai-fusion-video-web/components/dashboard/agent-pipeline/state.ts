import type { AiChatStreamEvent } from "@/lib/api/ai-pipeline";
import type {
  AgentPipelineState,
  SubTimelineItem,
  TimelineItem,
} from "./types";

export function createInitialPipelineState(): AgentPipelineState {
  return {
    status: "idle",
    reasoningText: "",
    timeline: [],
  };
}

export function createPendingPipelineState(): AgentPipelineState {
  return {
    status: "reasoning",
    reasoningText: "",
    timeline: [],
  };
}

function appendReasoningToSubTimeline(
  children: SubTimelineItem[],
  reasoningContent: string
): SubTimelineItem[] {
  const last = children[children.length - 1];
  if (last && last.type === "reasoning") {
    return [
      ...children.slice(0, -1),
      { ...last, text: last.text + reasoningContent },
    ];
  }

  return [...children, { type: "reasoning", text: reasoningContent }];
}

function updateLastSubTimelineReasoningDuration(
  children: SubTimelineItem[],
  durationMs: number
): SubTimelineItem[] {
  for (let index = children.length - 1; index >= 0; index--) {
    const item = children[index];
    if (item.type === "reasoning") {
      return children.map((child, childIndex) =>
        childIndex === index && child.type === "reasoning"
          ? { ...child, durationMs }
          : child
      );
    }
  }

  return children;
}

function appendReasoningToTimeline(
  timeline: TimelineItem[],
  reasoningContent: string
): TimelineItem[] {
  const last = timeline[timeline.length - 1];
  if (last && last.type === "reasoning") {
    return [
      ...timeline.slice(0, -1),
      { ...last, text: last.text + reasoningContent },
    ];
  }

  return [...timeline, { type: "reasoning", text: reasoningContent }];
}

function updateLastTimelineReasoningDuration(
  timeline: TimelineItem[],
  durationMs: number
): TimelineItem[] {
  for (let index = timeline.length - 1; index >= 0; index--) {
    const item = timeline[index];
    if (item.type === "reasoning") {
      return timeline.map((timelineItem, timelineIndex) =>
        timelineIndex === index && timelineItem.type === "reasoning"
          ? { ...timelineItem, durationMs }
          : timelineItem
      );
    }
  }

  return timeline;
}

function updateToolStatus(
  timeline: TimelineItem[],
  toolCallId: string,
  status: "calling" | "done" | "error",
  options?: {
    name?: string;
    result?: string;
    agentName?: string;
  }
): TimelineItem[] {
  let found = false;
  const updated = timeline.map((item) => {
    if (item.type !== "tool" || item.id !== toolCallId) {
      return item;
    }
    found = true;
    return {
      ...item,
      status,
      ...(options?.result !== undefined ? { result: options.result } : {}),
      ...(options?.name && isPlaceholderToolName(item.name)
        ? { name: options.name }
        : {}),
      ...(options?.agentName && !item.agentName
        ? { agentName: options.agentName }
        : {}),
    };
  });

  if (found) {
    return updated;
  }

  return [
    ...updated,
    {
      type: "tool",
      id: toolCallId,
      name: options?.name || "sub_agent",
      arguments: "",
      status,
      result: options?.result,
      agentName: options?.agentName,
      children: [],
    },
  ];
}

function isPlaceholderToolName(name?: string): boolean {
  return (
    !name ||
    name === "unknown_sub_agent" ||
    name === "sub_agent" ||
    name === "agent_spawn" ||
    name === "agent_send" ||
    name === "agent_list"
  );
}

function subAgentParentName(event: AiChatStreamEvent): string {
  return event.agentName || event.toolName || "sub_agent";
}

function appendToToolChildren(
  timeline: TimelineItem[],
  parentToolCallId: string,
  updater: (children: SubTimelineItem[]) => SubTimelineItem[],
  options?: {
    placeholderName?: string;
    agentName?: string;
  }
): TimelineItem[] {
  let found = false;
  const updated = timeline.map((item) => {
    if (item.type !== "tool" || item.id !== parentToolCallId) {
      return item;
    }
    found = true;
    return {
      ...item,
      ...(options?.placeholderName && isPlaceholderToolName(item.name)
        ? { name: options.placeholderName }
        : {}),
      ...(options?.agentName && !item.agentName
        ? { agentName: options.agentName }
        : {}),
      children: updater(item.children ?? []),
    };
  });

  if (found) {
    return updated;
  }

  return [
    ...updated,
    {
      type: "tool",
      id: parentToolCallId,
      name: options?.placeholderName || "sub_agent",
      arguments: "",
      status: "calling",
      agentName: options?.agentName,
      children: updater([]),
    },
  ];
}

function appendContentToSubTimeline(
  children: SubTimelineItem[],
  content: string,
  reasoningDurationMs?: number
): SubTimelineItem[] {
  let updated = [...children];
  if (reasoningDurationMs) {
    updated = updateLastSubTimelineReasoningDuration(updated, reasoningDurationMs);
  }
  const last = updated[updated.length - 1];
  if (last && last.type === "content") {
    return [
      ...updated.slice(0, -1),
      { ...last, text: last.text + content },
    ];
  }

  return [...updated, { type: "content", text: content }];
}

function appendContentToTimeline(
  timeline: TimelineItem[],
  content: string
): TimelineItem[] {
  const last = timeline[timeline.length - 1];
  if (last && last.type === "content") {
    return [
      ...timeline.slice(0, -1),
      { ...last, text: last.text + content },
    ];
  }

  return [...timeline, { type: "content", text: content }];
}

/**
 * 用完整的格式正确文本替换 timeline 中最后一个 content 条目的文本。
 * 用于 CONTENT_REPLACE 事件（REASONING isLast=true 提供的权威文本）。
 */
function replaceContentInTimeline(
  timeline: TimelineItem[],
  fullContent: string
): TimelineItem[] {
  // 从后往前找最后一个 content 类型条目
  for (let i = timeline.length - 1; i >= 0; i--) {
    const item = timeline[i];
    if (item.type === "content") {
      return timeline.map((t, idx) =>
        idx === i && t.type === "content"
          ? { ...t, text: fullContent }
          : t
      );
    }
  }
  // 没有已存在的 content 条目，不添加（不应发生）
  return timeline;
}

function replaceContentInSubTimeline(
  children: SubTimelineItem[],
  fullContent: string
): SubTimelineItem[] {
  for (let i = children.length - 1; i >= 0; i--) {
    const item = children[i];
    if (item.type === "content") {
      return children.map((c, idx) =>
        idx === i && c.type === "content"
          ? { ...c, text: fullContent }
          : c
      );
    }
  }
  return children;
}

export function reducePipelineEvent(
  prev: AgentPipelineState,
  event: AiChatStreamEvent
): AgentPipelineState {
  const next: AgentPipelineState = {
    ...prev,
    timeline: [...prev.timeline],
  };

  if (event.conversationId) {
    next.conversationId = event.conversationId;
  }

  const isSubAgent = !!event.parentToolCallId;

  switch (event.outputType) {
    case "REASONING":
      if (event.reasoningContent) {
        if (isSubAgent) {
          next.timeline = appendToToolChildren(
            next.timeline,
            event.parentToolCallId!,
            (children) =>
              appendReasoningToSubTimeline(children, event.reasoningContent!),
            {
              placeholderName: subAgentParentName(event),
              agentName: event.agentName,
            }
          );
        } else {
          next.status = "reasoning";
          next.reasoningText += event.reasoningContent;
          next.timeline = appendReasoningToTimeline(
            next.timeline,
            event.reasoningContent
          );
        }
      }
      return next;

    case "CONTENT":
      next.status = "running";
      if (event.reasoningDurationMs && !isSubAgent) {
        next.reasoningDurationMs = event.reasoningDurationMs;
        next.timeline = updateLastTimelineReasoningDuration(
          next.timeline,
          event.reasoningDurationMs
        );
      }
      if (event.content) {
        if (isSubAgent) {
          next.timeline = appendToToolChildren(
            next.timeline,
            event.parentToolCallId!,
            (children) =>
              appendContentToSubTimeline(
                children,
                event.content!,
                event.reasoningDurationMs
              ),
            {
              placeholderName: subAgentParentName(event),
              agentName: event.agentName,
            }
          );
        } else {
          next.timeline = appendContentToTimeline(next.timeline, event.content);
        }
      }
      return next;

    case "CONTENT_REPLACE":
      // REASONING isLast=true 提供格式正确的完整文本，替换之前 chunk 累积的（缺换行的）content
      if (event.content) {
        if (isSubAgent) {
          next.timeline = appendToToolChildren(
            next.timeline,
            event.parentToolCallId!,
            (children) =>
              replaceContentInSubTimeline(children, event.content!),
            {
              placeholderName: subAgentParentName(event),
              agentName: event.agentName,
            }
          );
        } else {
          next.timeline = replaceContentInTimeline(
            next.timeline,
            event.content
          );
        }
      }
      return next;

    case "TOOL_CALL":
      next.status = "running";
      if (event.toolCalls) {
        for (const toolCall of event.toolCalls) {
          if (isSubAgent) {
            next.timeline = appendToToolChildren(
              next.timeline,
              event.parentToolCallId!,
              (children) => {
                const existing = children.find(
                  (child) => child.type === "tool" && child.id === toolCall.id
                );
                if (existing?.type === "tool") {
                  return children.map((child) =>
                    child.type === "tool" && child.id === toolCall.id
                      ? {
                          ...child,
                          name: toolCall.name || child.name,
                          arguments: toolCall.arguments || child.arguments,
                          status: "calling",
                        }
                      : child
                  );
                }
                return [
                  ...children,
                  {
                    type: "tool",
                    id: toolCall.id,
                    name: toolCall.name,
                    arguments: toolCall.arguments,
                    status: "calling",
                  },
                ];
              },
              {
                placeholderName: subAgentParentName(event),
                agentName: event.agentName,
              }
            );
          } else if (
            !next.timeline.some(
              (item) => item.type === "tool" && item.id === toolCall.id
            )
          ) {
            next.timeline.push({
              type: "tool",
              id: toolCall.id,
              name: toolCall.name,
              arguments: toolCall.arguments,
              status: "calling",
              agentName: event.agentName,
            });
          } else {
            next.timeline = next.timeline.map((item) =>
              item.type === "tool" && item.id === toolCall.id
                ? {
                    ...item,
                    name: toolCall.name || item.name,
                    arguments: toolCall.arguments || item.arguments,
                    status: "calling",
                    agentName: item.agentName || event.agentName,
                  }
                : item
            );
          }
        }
      }
      return next;

    case "TOOL_FINISHED":
      if (event.toolCallId) {
        const toolItemStatus: "done" | "error" =
          event.toolStatus === "error" ? "error" : "done";
        if (isSubAgent) {
          next.timeline = appendToToolChildren(
            next.timeline,
            event.parentToolCallId!,
            (children) => {
              let found = false;
              const updated = children.map((child) => {
                if (child.type !== "tool" || child.id !== event.toolCallId) {
                  return child;
                }
                found = true;
                return {
                  ...child,
                  name: event.toolName || child.name,
                  status: toolItemStatus,
                  result: event.toolResult,
                };
              });
              if (found) {
                return updated;
              }
              return [
                ...updated,
                {
                  type: "tool",
                  id: event.toolCallId!,
                  name: event.toolName || "tool",
                  arguments: "",
                  status: toolItemStatus,
                  result: event.toolResult,
                },
              ];
            },
            {
              placeholderName: subAgentParentName(event),
              agentName: event.agentName,
            }
          );
        } else {
          next.timeline = updateToolStatus(
            next.timeline,
            event.toolCallId,
            toolItemStatus,
            {
              name: event.toolName,
              result: event.toolResult,
              agentName: event.agentName,
            }
          );
        }
      }
      return next;

    case "SUB_AGENT_FINISHED":
      if (isSubAgent) {
        next.timeline = updateToolStatus(
          next.timeline,
          event.parentToolCallId!,
          "done",
          {
            name: subAgentParentName(event),
            agentName: event.agentName,
          }
        );
      }
      return next;

    case "DONE":
      next.status = "done";
      if (event.content) {
        next.timeline = appendContentToTimeline(next.timeline, event.content);
      }
      return next;

    case "ERROR":
      if (isSubAgent) {
        next.timeline = updateToolStatus(
          next.timeline,
          event.parentToolCallId!,
          "error",
          {
            name: subAgentParentName(event),
            agentName: event.agentName,
          }
        );
        next.timeline = appendToToolChildren(
          next.timeline,
          event.parentToolCallId!,
          (children) => [
            ...children,
            {
              type: "content",
              text: `❌ ${event.agentName || "子Agent"} 出错: ${event.error || "未知错误"}`,
            },
          ],
          {
            placeholderName: subAgentParentName(event),
            agentName: event.agentName,
          }
        );
      } else {
        next.status = "error";
        next.error = event.error || "未知错误";
      }
      return next;

    case "CANCELLED":
      next.status = "cancelled";
      return next;

    default:
      return next;
  }
}
