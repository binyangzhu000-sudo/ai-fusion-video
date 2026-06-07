package com.stonewu.fusion.service.ai;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import cn.hutool.core.util.StrUtil;
import com.stonewu.fusion.entity.ai.AgentMessage;
import com.stonewu.fusion.mapper.ai.AgentMessageMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import java.util.ArrayList;

/**
 * Agent 消息服务
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AgentMessageService {

    private final AgentMessageMapper messageMapper;

    public List<AgentMessage> listByConversation(String conversationId) {
        return messageMapper.selectList(new LambdaQueryWrapper<AgentMessage>()
                .eq(AgentMessage::getConversationId, conversationId)
                .orderByAsc(AgentMessage::getMessageOrder));
    }

    public AgentMessage saveUserMessage(String conversationId, String content, String referencesJson) {
        int nextOrder = messageMapper.findMaxMessageOrder(conversationId) + 1;
        AgentMessage message = AgentMessage.builder()
                .conversationId(conversationId)
                .role("user")
                .content(content)
                .referencesJson(referencesJson)
                .messageOrder(nextOrder)
                .build();
        messageMapper.insert(message);
        return message;
    }

    public AgentMessage saveAssistantMessage(String conversationId, String content,
                                             String reasoningContent, Long reasoningDurationMs) {
        return saveAssistantMessage(conversationId, content, reasoningContent,
                reasoningDurationMs, null);
    }

    public AgentMessage saveAssistantMessage(String conversationId, String content,
                                             String reasoningContent, Long reasoningDurationMs,
                                             String parentToolCallId) {
        boolean hasContent = StrUtil.isNotEmpty(content);
        boolean hasReasoning = StrUtil.isNotEmpty(reasoningContent);
        if (!hasContent && !hasReasoning) {
            return null;
        }
        int nextOrder = messageMapper.findMaxMessageOrder(conversationId) + 1;
        AgentMessage message = AgentMessage.builder()
                .conversationId(conversationId)
                .role("assistant")
                .content(hasContent ? content : null)
                .parentToolCallId(parentToolCallId)
                .reasoningContent(hasReasoning ? reasoningContent : null)
                .reasoningDurationMs(reasoningDurationMs)
                .messageOrder(nextOrder)
                .build();
        messageMapper.insert(message);
        return message;
    }

    /**
     * 更新已保存消息的 content 字段。
     * 用于 REASONING isLast=true 事件提供格式正确的完整文本替换
     * 之前由增量 chunk 累积的（可能丢失换行的）内容。
     */
    public void updateMessageContent(Long messageId, String content) {
        if (messageId == null) {
            return;
        }
        AgentMessage update = new AgentMessage();
        update.setId(messageId);
        update.setContent(content);
        messageMapper.updateById(update);
    }

    public AgentMessage saveToolCall(String conversationId, String toolName,
                                     String toolStatus, String content,
                                     String toolCallId, String parentToolCallId) {
        int nextOrder = messageMapper.findMaxMessageOrder(conversationId) + 1;
        AgentMessage message = AgentMessage.builder()
                .conversationId(conversationId)
                .role("tool")
                .toolName(toolName)
                .toolStatus(toolStatus)
                .content(content)
                .toolCallId(toolCallId)
                .parentToolCallId(parentToolCallId)
                .messageOrder(nextOrder)
                .build();
        messageMapper.insert(message);
        return message;
    }

    /**
     * 从数据库中加载消息并转换为前端流式响应事件，用于 Redis 缓存失效后的强壮兜底。
     */
    public List<AiChatStreamRespVO> getHistoricEvents(String conversationId) {
        List<AgentMessage> messages = listByConversation(conversationId);
        List<AiChatStreamRespVO> events = new ArrayList<>();
        for (AgentMessage msg : messages) {
            if ("user".equals(msg.getRole())) {
                events.add(new AiChatStreamRespVO()
                        .setConversationId(conversationId)
                        .setOutputType("CONTENT")
                        .setContent(msg.getContent()));
            } else if ("assistant".equals(msg.getRole())) {
                if (StrUtil.isNotEmpty(msg.getReasoningContent())) {
                    events.add(new AiChatStreamRespVO()
                            .setConversationId(conversationId)
                            .setOutputType("REASONING")
                            .setReasoningContent(msg.getReasoningContent())
                            .setReasoningDurationMs(msg.getReasoningDurationMs())
                            .setParentToolCallId(msg.getParentToolCallId()));
                }
                if (StrUtil.isNotEmpty(msg.getContent())) {
                    events.add(new AiChatStreamRespVO()
                            .setConversationId(conversationId)
                            .setOutputType("CONTENT")
                            .setContent(msg.getContent())
                            .setReasoningDurationMs(msg.getReasoningDurationMs())
                            .setParentToolCallId(msg.getParentToolCallId()));
                }
            } else if ("tool".equals(msg.getRole())) {
                events.add(new AiChatStreamRespVO()
                        .setConversationId(conversationId)
                        .setOutputType("TOOL_FINISHED")
                        .setToolCallId(msg.getToolCallId())
                        .setToolName(msg.getToolName())
                        .setToolStatus(msg.getToolStatus())
                        .setToolResult(msg.getContent())
                        .setParentToolCallId(msg.getParentToolCallId()));
            }
        }
        return events;
    }
}
