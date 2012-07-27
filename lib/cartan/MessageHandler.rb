module Cartan

  class MessageHandler

    def initialize(node, state = proc{ "none" }, &block)
      @node = node
      @state = state

      handle(&block) if block_given?
    end

    def receive(uuid, label, message)
      current_state = state.call

      states[current_state].call(uuid, label, message) if states.has_key? current_state
      states[all].call(uuid, label, message) if states.has_key? all
    end

    def handle(&block)
      instance_eval(&block) if block_given?
    end

    def state(which_state, &block)
      raise InvalidState unless which_state.is_a?(Symbol) or which_state == all?

      states[which_state] = block
    end

    def states
      @states ||= {}
    end

    def all
      "all"
    end

  end

end