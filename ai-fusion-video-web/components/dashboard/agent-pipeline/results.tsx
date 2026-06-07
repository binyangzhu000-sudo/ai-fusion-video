"use client";

// Agent-pipeline 的工具结果渲染直接复用 notification-panel 的完整实现，
// 避免维护两份不同步的 switch-case 导致部分工具结果显示为原始 JSON。
import { ToolResultDisplay as NotificationToolResultDisplay } from "@/components/dashboard/notification-panel/results";

/**
 * agent-pipeline 的 ToolResultDisplay 只是一层 prop 适配器：
 *   - agent-pipeline 使用 `result` prop
 *   - notification-panel 使用 `content` prop
 */
export function ToolResultDisplay({ toolName, result }: { toolName: string; result: string }) {
  return <NotificationToolResultDisplay toolName={toolName} content={result} />;
}
