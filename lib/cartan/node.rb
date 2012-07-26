module Cartan
  class Node; end
end

require 'cartan/node/messaging'

module Cartan

  class Node
    attr_accessor :config, :uuid

    def initialize(config)
      @config = config

      configure!
    end

    def run
      EM.synchrony do
        connect!
        @msg.subscribe(exclusive, &method(:process_exclusive))

        yield if block_given?
      end
    end

    def stop
      disconnect!
      yield if block_given?
      EM.stop
    end

    def configure!
      capture_signals
      setup_log
      @uuid = ::SecureRandom.uuid
    end

    def connect!
      msg.open
    end

    def disconnect!
      msg.close
    end

    # Configures logging for this node.
    def setup_log
      Cartan::Log.loggers << Logger.new(@config[:log_location])
      Cartan::Log.level = @config[:log_level]
    end

    # Captures interupt signals.
    def capture_signals
      %w[ TERM INT QUIT HUP ].each do |signal|
        Signal.trap(signal) { 
          Cartan::Log.error "SIG#{signal} received, stopping."
          stop
        }
      end
    end

    def process_exclusive(type, message)

      case type
      when "node.type.request"
        msg.send_node(message["uuid"], "node.type.response", { "type" => node_type })
      when "node.heartbeat.request"
        msg.send_node(message["uuid"], "node.heartbeat.response", "")
      end

    end

    def msg
      @msg ||= Cartan::Node::Messaging.new(@uuid, @config[:namespace], @config[:amqp])
    end

    def node_type
      "node"
    end

    def exclusive
      @exclusive ||= msg.exclusive
    end

  end

end