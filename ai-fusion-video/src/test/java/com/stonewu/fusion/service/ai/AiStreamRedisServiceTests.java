package com.stonewu.fusion.service.ai;

import com.stonewu.fusion.controller.ai.vo.AiChatStreamRespVO;
import com.stonewu.fusion.service.ai.AiStreamRedisService.StreamEventAccumulator;
import com.stonewu.fusion.service.ai.AiStreamRedisService.StreamEventAccumulator.AccumulatedEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AiStreamRedisServiceTests {

    @Test
    void mainAgentTerminalEventOnlyMatchesConversationLevelTerminals() {
        AiChatStreamRespVO mainDone = new AiChatStreamRespVO().setOutputType("DONE");
        AiChatStreamRespVO mainError = new AiChatStreamRespVO().setOutputType("ERROR");
        AiChatStreamRespVO mainCancelled = new AiChatStreamRespVO().setOutputType("CANCELLED");
        AiChatStreamRespVO subAgentError = new AiChatStreamRespVO()
                .setOutputType("ERROR")
                .setAgentName("episode_storyboard_writer")
                .setParentToolCallId("call_1");
        AiChatStreamRespVO unmappedSubAgentError = new AiChatStreamRespVO()
                .setOutputType("ERROR")
                .setAgentName("episode_storyboard_writer");

        assertThat(AiStreamRedisService.isMainAgentTerminalEvent(mainDone)).isTrue();
        assertThat(AiStreamRedisService.isMainAgentTerminalEvent(mainError)).isTrue();
        assertThat(AiStreamRedisService.isMainAgentTerminalEvent(mainCancelled)).isTrue();
        assertThat(AiStreamRedisService.isMainAgentTerminalEvent(subAgentError)).isFalse();
        assertThat(AiStreamRedisService.isMainAgentTerminalEvent(unmappedSubAgentError)).isFalse();
    }

    @Test
    void mainAgentErrorAndCancelledIgnoreSubAgentEvents() {
        AiChatStreamRespVO mainError = new AiChatStreamRespVO().setOutputType("ERROR");
        AiChatStreamRespVO mainCancelled = new AiChatStreamRespVO().setOutputType("CANCELLED");
        AiChatStreamRespVO subAgentError = new AiChatStreamRespVO()
                .setOutputType("ERROR")
                .setAgentName("episode_storyboard_writer")
                .setParentToolCallId("call_1");
        AiChatStreamRespVO subAgentCancelled = new AiChatStreamRespVO()
                .setOutputType("CANCELLED")
                .setAgentName("episode_storyboard_writer")
                .setParentToolCallId("call_1");

        assertThat(AiStreamRedisService.isMainAgentErrorEvent(mainError)).isTrue();
        assertThat(AiStreamRedisService.isMainAgentCancelledEvent(mainCancelled)).isTrue();
        assertThat(AiStreamRedisService.isMainAgentErrorEvent(subAgentError)).isFalse();
        assertThat(AiStreamRedisService.isMainAgentCancelledEvent(subAgentCancelled)).isFalse();
    }

    @Test
    void replayAccumulatorSeparatesSubAgentTokensByAgentName() {
        StreamEventAccumulator accumulator = new StreamEventAccumulator("conversation-1");

        AiChatStreamRespVO writerToken = new AiChatStreamRespVO()
                .setMessageId("message-1")
                .setConversationId("conversation-1")
                .setOutputType("CONTENT")
                .setParentToolCallId("parent-call-1")
                .setAgentName("episode_writer")
                .setContent("writer-1");
        AiChatStreamRespVO reviewerToken = new AiChatStreamRespVO()
                .setMessageId("message-1")
                .setConversationId("conversation-1")
                .setOutputType("CONTENT")
                .setParentToolCallId("parent-call-1")
                .setAgentName("episode_reviewer")
                .setContent("reviewer-1");
        AiChatStreamRespVO writerToken2 = new AiChatStreamRespVO()
                .setMessageId("message-1")
                .setConversationId("conversation-1")
                .setOutputType("CONTENT")
                .setParentToolCallId("parent-call-1")
                .setAgentName("episode_writer")
                .setContent("writer-2");

        assertThat(accumulator.accumulate(writerToken, "1-0")).isEmpty();
        List<AccumulatedEvent> flushedByReviewer = accumulator.accumulate(reviewerToken, "2-0");
        List<AccumulatedEvent> flushedByWriterAgain = accumulator.accumulate(writerToken2, "3-0");
        AccumulatedEvent remaining = accumulator.flush();

        assertThat(flushedByReviewer).hasSize(1);
        assertThat(flushedByReviewer.get(0).getEvent().getAgentName()).isEqualTo("episode_writer");
        assertThat(flushedByReviewer.get(0).getEvent().getContent()).isEqualTo("writer-1");

        assertThat(flushedByWriterAgain).hasSize(1);
        assertThat(flushedByWriterAgain.get(0).getEvent().getAgentName()).isEqualTo("episode_reviewer");
        assertThat(flushedByWriterAgain.get(0).getEvent().getContent()).isEqualTo("reviewer-1");

        assertThat(remaining).isNotNull();
        assertThat(remaining.getEvent().getAgentName()).isEqualTo("episode_writer");
        assertThat(remaining.getEvent().getContent()).isEqualTo("writer-2");
    }
}
