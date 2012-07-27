require 'em-synchrony/amqp'
require 'msgpack'

require 'cartan/message-handler'

module Cartan

  # A service for sending and receiving messages.
  class Messaging
    attr_accessor :amqp, :channel, :exchange

    MP = MessagePack
    AMQP = EM::Synchrony::AMQP

    HandlerCollection = Struct.new(:queue, :handlers)

    # Initializes a messaging service.
    #
    # @param [String] uuid The uuid of the node attached to.
    # @param [String] namespace The name of the namespace to operate under.
    # @param [Hash] config AMQP configuration settings.
    def initialize(uuid, namespace = "", config = {})
      @uuid = uuid
      @namespace = namespace
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
      @exchange = @channel.direct(@namespace, :auto_delete => true)

    end

    # Stops the messaging service.
    def stop
      subscriptions.each { |n, v| unsubscribe(n) }

      @amqp.close
    end

    # Creates and subscribes to queue.
    #
    # @param [String] name The name of the queue to subscribe to.
    # @param [Hash] config Queue creations options.
    def subscribe(name, config = {})
      return if subscriptions.has_key? name

      queue = @channel.queue(ns(name), {:auto_delete => true}.merge!(config))
      queue.bind(@exchange, :routing_key => ns(name)) 

      queue.subscribe do |headers, payload|
        decoded = MP.unpack(payload)
        subscriptions[name].handlers.each do |h|
          h.receive(decoded["uuid"], headers.type, decoded["msg"])
        end
      end

      subscriptions[name] = HandlerCollection.new(queue, [])
    end

    # Unsubscribes from a queue.
    #
    # @param [String] name The name of the queue to unsubscribe from.
    def unsubscribe(name)
      if subscriptions.has_key? name
        subscriptions[name].queue.unsubscribe(:nowait => false)
        subscriptions.delete(name)
      end
    end

    # Attach a message handler to a queue. Creates and subscribes to the queue if
    # it doesn't already exist.
    #
    # @param [String] name The name of the queue to attach the handler to.
    # @param [Cartan::MessageHandler] handler The handler that will receive all 
    # new messages
    def add_handler(name, handler)
      subscribe(name)
      subscriptions[name].handlers << handler
    end

    # Removes the handler from the queue.
    #
    # @param [String] name The name of the queue from which to remove the handler.
    # @param [Cartan::MessageHandler] handler The handler to remove
    def remove_handler(name, handler)
      return unless subscriptions.has_key? name
      subscriptions[name].handlers.delete(handler)
    end

    # Sends a message to a queue.
    #
    # @param [String] name The name of the queue to send the message to.
    # @param [String] label The message label.
    # @param [String Hash] message The message to send.
    def send_message(name, label, message = "")
      encoded = MP.pack({ :uuid => @uuid, :msg => message })
      @exchange.publish(encoded, :type => label, :routing_key => ns(name))
    end

    # A hash of all the maintained subscriptions.
    def subscriptions
      @subscriptions ||= {}
    end

    # Creates and subscribes to this node's exclusive queue.
    def handle_exclusive(handler)
      name = exclusive(@uuid)

      subscribe(name, :exclusive => true)
      add_handler(name, handler)
    end

    # Sends a message to a node's exclusive queue.
    # @see Messaging#send_message
    #
    # @param [String] The uuid of the node to message.
    # @param [String] label
    # @param [String Hash] message
    def send_node(uuid, label, message = "")
      send_message(exclusive(uuid), label, message)
    end

    # Gets the name of a node's exclusive queue.
    #
    # @param [String] uuid The uuid of the desired node.
    def exclusive(uuid)
      jn("exclusive", uuid)
    end

    # Joins together words with a period.
    #
    # @param [String] *words The words to join together.
    def jn(*words)
      words.flatten.join(".")
    end

    # Similar to jn but adds the namespace in the front.
    def ns(*words)
      jn(@namespace, words)
    end

  end

end