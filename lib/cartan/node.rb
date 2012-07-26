require 'state_machine'
require 'em-synchrony'
require 'securerandom'

require 'cartan/messaging'

module Cartan

  # Base class for all nodes on the network. Manages event loop and provides a
  # messaging service.
  class Node
    attr_accessor :config

    # A state machine that drives the node
    state_machine :node_state, :initial => :idle do
      trans :idle, :started!, :running, :run
      trans :running, :stopped!, :idle, :quit
    end

    # Initializes the node
    #
    # @param [Hash] config A hash-like object containing this node's
    #                      configuration settings
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
        @msg = Cartan::Messaging.new(@config[:namespace], @config[:amqp]).start

        yield if block_given?
        started!
      end
    end

    # Stops the node's event loop.
    #
    # @yield [] A block to be run after the event loop has been stopped.
    def stop
      @msg.stop
      EM.stop

      yield if block_given?
      stopped!
    end

    # A node's event loop
    def run; end

    # Configures logging for this node based of the config.
    def setup_logging
      Cartan::Log.loggers << Logger.new(Cartan::Config[:log_location])
      Cartan::Log.level = Cartan::Config[:log_level]
    end

    # Captures interupt signals for this process and stops the node.
    def capture_signals
      %w[ TERM INT QUIT HUP ].each do |signal|
        Signal.trap(signal) { 
          Cartan::Log.error "SIG#{signal} received, stopping."
          stop if running?
        }
      end
    end

  end

end