package com.stonewu.fusion.service.ai.agentscope;

/**
 * Raised when a user cancellation should interrupt the AgentScope V2 event stream or tool call.
 */
public class AgentCancelledException extends RuntimeException {

    public AgentCancelledException(String message) {
        super(message);
    }
}
