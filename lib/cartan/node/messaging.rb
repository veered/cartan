require 'em-synchrony/amqp'
require 'msgpack'

MP = MessagePack

module Cartan

	class Node::Messaging
    attr_accessor :uuid, :config, :amqp, :channel

    def initialize(uuid, namespace, config = {})
      @uuid = uuid
      @namespace = namespace
      @config = config
    end

    # Opens a connection to AMQP
    def open
      @amqp = EM::Synchrony::AMQP.connect(@config)
      @channel = EM::Synchrony::AMQP::Channel.new(@amqp)
      self
    end

    # Closes the connection to AMQP
    def close
      @amqp.close
    end

    # Subscribe to the given queue.
    #
    # @param [Queue] The queue to subscribe to
    # @yield The block to be called when a message is received
    # @yieldparam [String] The type of message
    # @yieldparam [Hash] The actual content of the message
    def subscribe(queue, &block)
      queue.subscribe { |headers, payload| block.call(headers.type, MP.unpack(payload)) }
    end

    # Unsubscribes from the given queue
    #
    # @param [Queue] The queue to unsubscribe to
    def unsubscribe(queue)
      queue.unsubscribe
    end

    def create_queue(name)
      @channel.queue(ns(name))
    end

    # Sends a message to a node's private queue.
    #
    # @param [String] uuid The node's uuid
    # @param [String] type The type of the message to send
    # @param [String] msg The message to send
    def send_message(routing_key, type, msg = "")
      @channel.default_exchange.
        publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
            :type => type,
            :routing_key => routing_key
        )
    end

    def send_node(uuid, type, msg = "")
      send_message(ns("exclusive", uuid), type, msg)
    end

    # Namespaces the list of input arguments.
    #
    # @param [String] *args The parameters to namespace.
    def ns(*words)
      words.flatten.insert(0, @namespace.to_s).join(".")
    end

    # This node's exclusive queue.
    def exclusive
      @exclusive ||= @channel.queue(ns("exclusive", @uuid), :exclusive => true)
    end


	end

end