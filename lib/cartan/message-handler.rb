module Cartan

  # A helper for performing stateful message routing.
  class MessageHandler

    # Initializes a MessageHandler instance.
    #
    # @param [Cartan::Node] node The node that this message handler is attached to.
    # @param [Callable] node A callable which returns the current state.
    # @yield [] (see Cartan::MessageHandler::handle) A block of code to be 
    # evaluated in the context of the handler.
    def initialize(node, state = proc{ "none" }, &block)
      @node = node
      @state = state

      handle(&block) if block_given?
    end

    # The message handler hook. Decides which handler(s) to use based on the
    # current state.
    # 
    # @see Cartan::Messaging#subscribe Clarifies what the parameters mean.
    def receive(uuid, label, message)
      current_state = @state.call
      
      states[current_state].call(uuid, label, message) if states.has_key? current_state
      states[all].call(uuid, label, message) if states.has_key? all
    end

    # Evaluates the block in the context of the handler. This is where messages
    # are handled
    def handle(&block)
      instance_eval(&block) if block_given?
    end

    # Defines a new message handler, dependent on the current state.
    def state(which_state, &block)
      unless which_state.is_a?(Symbol) or which_state == all
        raise Cartan::Exception::InvalidState,
          "The state provided was not a symbol or all!"
      end

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