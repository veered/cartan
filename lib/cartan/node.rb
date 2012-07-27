require 'state_machine'
require 'em-synchrony'
require 'securerandom'

require 'cartan/messaging'

module Cartan

  # Base class for all nodes on the network. Manages event loop and provides a
  # messaging service.
  class Node
    attr_accessor :config, :uuid, :msg

    # A state machine that drives the node
    state_machine :node_state, :initial => :idle do
      before_transition :idle => :running, :do => :run
      before_transition :running => :idle, :do => :quit

      event(:started!) { transition :idle => :running }
      event(:stopped!) { transition :running => :idle }
    end

    # Initializes the node
    #
    # @param [Hash] config A hash-like object containing this node's
    # configuration settings
    def initialize(config)
      super() # Initializes the state machine

      @config = config

      @uuid = SecureRandom.hex
      setup_logging
      capture_signals
    end

    # Starts the node's event loop.
    #
    # @yield [] A block to be run in the event loop.
    def start
      EM.synchrony do
        @msg = Cartan::Messaging.new(@uuid, @config[:namespace], @config[:amqp])
        @msg.start

        @msg.handle_exclusive(exclusive_handler)

        started!
        yield if block_given?
      end
    end

    # Stops the node's event loop.
    #
    # @yield [] A block to be run after the event loop has been stopped.
    def stop
      yield if block_given?
      @msg.stop
      EM.stop

      stopped!
    end

    # A node's event loop
    def run; end

    # Actions to be taken on node exit
    def quit; end

    # Configures logging for this node based of the config.
    def setup_logging
      Cartan::Log.loggers << Logger.new(@config[:log_location])
      Cartan::Log.level = @config[:log_level]
    end

    # Captures interupt signals for this process and stops the node.
    def capture_signals
      %w[ TERM INT QUIT HUP ].each do |signal|
        Signal.trap(signal) { 

          EM::Synchrony.next_tick do
            Cartan::Log.error "SIG#{signal} received, stopping."
            stop if running?
          end

        }
      end
    end

    # Convenience method for identifying node types.
    def type
      self.class.name.split("::").last
    end

    # A hash containing info about this node
    def info
      { :type => type }
    end

    def exclusive_handler
      @exclusive_handler ||= Cartan::MessageHandler.new(self, proc{ node_state.to_sym }) do

        handle "heartbeat", :running do |uuid, label, message|
            @node.msg.send_node(uuid, "heartbeat.response", @node.info)
        end

      end
    end

  end

end