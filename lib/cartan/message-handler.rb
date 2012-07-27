module Cartan

  # A helper for performing stateful message routing.
  class MessageHandler

    Handler = Struct.new(:pattern, :hook)

    # Initializes a MessageHandler instance.
    #
    # @param [Cartan::Node] node The node that this message handler is attached to.
    # @param [Callable] node A callable which returns the current state.
    # @yield [] (see Cartan::MessageHandler::handle) A block of code to be 
    # evaluated in the context of the handler.
    def initialize(node, state = proc{ none }, &block)
      @node = node
      @state = state

      bind(&block) if block_given?
    end

    # The message handler hook. Decides which handler(s) to use based on the
    # current state.
    # 
    # @see Cartan::Messaging#subscribe Clarifies what the parameters mean.
    def receive(uuid, label, message)
      current_state = @state.call
      
      find_matches(label, states[current_state]).each do |h| 
        h.hook.call(uuid, label, message)
      end

      find_matches(label, states[all]).each do |h| 
        h.hook.call(uuid, label, message)
      end
    end

    # Evaluates the block in the context of the handler. This is where messages
    # are handled
    def bind(&block)
      instance_eval(&block) if block_given?
    end

    # Defines a new message handler, dependent on the given states.
    # 
    # @param [String Regexp] pattern The message label to handle
    # @param [Symbol Array] which_states The states to handle
    # @yield The block to be executed on any handled message.
    def handle(pattern, which_states = all, &block)
      handler = Handler.new(Regexp.new(pattern), block)
      [*which_states].each do |s|
        unless valid_state?(s)
          raise Cartan::Exception::InvalidState,
            "The state '#{s}'' was not a symbol or all!"
        end

        states[s] ||= []
        states[s] << handler
      end
    end

    def states
      @states ||= {}
    end

    def all
      "all"
    end

    def none
      "none"
    end

    private

      def find_matches(label, handlers)
        [*handlers].select{ |h| label =~ h.pattern }
      end

      def valid_state?(state)
        state.is_a?(Symbol) or state == all
      end

  end

end