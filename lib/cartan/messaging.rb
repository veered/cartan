require 'em-synchrony/amqp'
require 'msgpack'

module Cartan

  # A service for sending and receiving messages.
  class Messaging
    attr_accessor :amqp, :channel

    MP = MessagePack
    AMQP = EM::Synchrony::AMQP

    # Initializes a messaging service.
    #
    # @param [String] uuid The uuid of the node attached to.
    # @param [String] exchange The name of the exchange to operate under.
    # @param [Hash] config AMQP configuration settings.
    def initialize(uuid, exchange = "", config = {})
      @uuid = uuid
      @exchange_name = exchange
      @config = config
    end

    # Starts the messaging service.
    #
    # @raise [Cartan::Exception::ReactorNotRunning]
    def start
      unless EM.reactor_running?
        raise Cartan::Exception::ReactorNotRunning,
          "The EventMachine reactor is not running!"
      end

      @amqp = AMQP.connect(@config)
      @channel = AMQP::Channel.new(@amqp)
      @exchange = @channel.fanout(@exchange_name)

    end

    # Stops the messaging service.
    def stop
      @subscriptions.each { |n, v| unsubscribe(n) }

      @amqp.close
    end

    # Creates and subscribes to queue.
    #
    # @param [String] name The name of the queue to subscribe to.
    # @param [Cartan::MessageHandler] handler The handler that will receive all 
    # new messages
    # @param [Hash] config Queue creations options.
    def subscribe(name, handler, config = {})
      queue = @channel.queue(name, config)
      queue.bind(@exchange) 

      queue.subscribe do |headers, payload|
        decoded = MP.unpack(payload)
        handler.receive(decoded["uuid"], headers.type, decoded["msg"])
      end

      unsubscribe name
      @subscriptions[name] = queue
    end

    # Unsubscribes from a queue.
    #
    # @param [String] name The name of the queue to unsubscribe from.
    def unsubscribe(name)
      if @subscriptions.has_key? name
        @subscriptions[name].unsubscribe
      end
    end

    # Sends a message to a queue.
    #
    # @param [String] name The name of the queue to send the message to.
    # @param [String] label The message label.
    # @param [String Hash] message The message to send.
    def send_message(name, label, message)
      encoded = MP.pack({ :uuid => @uuid, :msg => message })
      @exchange.publish(encoded, :type => jn(label), :routing_key => name)
    end

    # A hash of all the maintained subscriptions.
    def subscriptions
      @subscriptions ||= {}
    end

    # Creates and subscribes to this node's exclusive queue.
    def subscribe_exclusive(handler)
      subscribe(exclusive(@uuid), handler, :exclusive => true)
    end

    # Sends a message to a node's exclusive queue.
    # @see Messaging#send_message
    #
    # @param [String] The uuid of the node to message.
    # @param [String] label
    # @param [String Hash] message
    def send_node(uuid, label, message)
      send_message(exclusive(uuid), label, message)
    end

    # Gets the name of a node's exclusive queue.
    #
    # @param [String] uuid The uuid of the desired node.
    def exclusive(uuid)
      jn("exclusive", uuid)
    end

    private

      # Joins together words with a period.
      #
      # @param [String] *words The words to join together.
      def jn(*words)
        words.flatten.join(".")
      end

  end

end