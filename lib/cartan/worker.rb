module Cartan

	class Worker

		attr_accessor :uuid, :amqp, :channel, :orchestrator

		state_machine :state, :initial => :initializing do
			after_transition [:initializing, :recovering] => :connecting, :do => :connect
			after_transition :connecting => :monitoring, :do => :monitor
			after_transition any => :recovering, :do => :recover
			after_transition [:monitoring, :recovering] => :disconnecting, :do => :disconnect

			event :initialized do
				transition :initializing => :connecting 
			end

			event :connected do
				transition :connecting => :monitoring 
			end

			event :error do
				transition all => :recovering
			end

			event :killed do
				transition all => :disconnecting
			end
		end

		# Initialize worker.
		# 
		# @param [String] config The path to the configuration file.
		def initialize(config)
			super()

			Cartan::Config.from_file(config, "json")

			Cartan::Log.loggers << Logger.new(Cartan::Config[:log_location])
			Cartan::Log.level = Cartan::Config[:log_level]

			Signal.trap("INT") { killed }

			@uuid = SecureRandom.uuid

			EM.synchrony do
				initialized
			end
		end

		# Connects to RabbitMQ.
		def connect
			connect_amqp
			declare @uuid

			connected
		end

		# Connects to RabbitMQ
		def connect_amqp
			@amqp = EM::S::AMQP.connect Cartan::Config[:amqp]
			@channel = EM::S::AMQP::Channel.new(@amqp)

			@orchestrator = @channel.fanout(ns "orchestrator")

			@orchestra = @channel.fanout(ns "orchestra")
			queue = @channel.queue(ns "orchestra")
			queue.bind(ns "orchestra")
			queue.subscribe { |args| process_broadcast(args[0], args[1]) }

			queue = @channel.queue(ns("message", @uuid), :exclusive => true)
			queue.subscribe { |args| process_exclusive(args[0], args[1]) }
		end

		# Attempts to gracefully close all connections
		def disconnect
			@amqp.close { EM.stop }
		end

		def monitor
			EM.add_periodic_timer(5) {
				send_orchestrator("worker.heartbeat", "Hello!")
			}
		end

		def process_broadcast(headers, payload)

		end

		def process_exclusive(headers, payload)
			message = MessagePack.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"
		end

		# Declares the worker to the orchestrator
		#
		# @param [String] uuid The uuid of the worker to declare
		def declare(uuid)
			send_orchestrator("worker.declare")
		end

		# Sends a message to the orchestrator
		# 
		# @param [String] type The type of the message to send
		# @param [String] msg The message to send
		def send_orchestrator(type, msg = "")		
			@orchestrator.publish(MessagePack.pack( { :uuid => @uuid, :msg => msg } ), 
									:type => type)
		end

		# Sends a message to a node.
		#
		# @param [String] uuid The node's uuid
		# @param [String] type The type of the message to send
		# @param [String] msg The message to send
		def send_message(uuid, type, msg = "")
			@channel.default_exchange.
				publish(MessagePack.pack( { :uuid => @uuid, :msg => msg } ), 
						:type => type,
						:routing_key => ns("message", uuid))
		end

		# Namespaces the list of input arguments.
		def ns(*words)
			words.flatten.insert(0, Cartan::Config[:namespace]).join(".")
		end

	end

end