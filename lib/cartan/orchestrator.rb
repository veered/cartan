module Cartan

	class Orchestrator

		attr_accessor :redis, :amqp, :channel, :orchestra

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

		# Initialize orchestrator.
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

		# Connects to and RabbitMQ.
		def connect
			connect_redis
			connect_amqp

			connected
		end

		# Connects to Redis
		def connect_redis
			@redis = EM::P::Redis.connect Cartan::Config[:redis]
		end

		# Connects to RabbitMQ
		def connect_amqp
			@amqp = EM::S::AMQP.connect Cartan::Config[:amqp]
			@channel = EM::S::AMQP::Channel.new(@amqp)

			@channel.fanout(ns "orchestrator")
			queue = @channel.queue(ns "orchestrator")
			queue.bind(ns "orchestrator")
			queue.subscribe { |args| process_orchestra(args[0], args[1]) }

			@orchestra = @channel.fanout(ns "orchestra")
		end

		# Attempts to gracefully close connections
		def disconnect
			@redis.unbind
			@amqp.close { EM.stop }
		end

		def monitor
			EM.add_periodic_timer
		end

		# Process new message
		#
		# @param [AMQP::Header] headers Headers (metadata) associated with this message (for example, routing key).
	    # @param [String] payload Message body (content). On Ruby 1.9, you may want to check or enforce content encoding.
		def process_orchestra(headers, payload)
			message = MessagePack.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "worker.declare"
				declare_worker message["uuid"]
				send_message(message["uuid"], "orchestrator.ack")
			when "worker.heartbeat"
				send_message(message["uuid"], "You're alive!!!")

			when "resource.declare"
				declare_resource message["uuid"]
				send_message message["uuid"], "orchestrator.config", { :redis => Cartan::Config[:redis] }
			when "resource.request_workers"
				workers = get_workers message["workers"]
				send_message message["uuid"], "orchestrator.assign", { :workers => workers }
			end

		end

		# Adds the worker to the pool of avaliable workers.
		#
		# @param [String] uuid The worker's uuid
		def declare_worker(uuid)
			@redis.sadd(ns("worker-pool"), uuid)
		end

		# Adds the resource to the list of resources.
		#
		# @param [String] uuid The resource's uuid
		def declare_resource(uuid)
			@redis.sadd(ns("resources"), uuid)
		end

		# Attempts to retrieve unassigned workers from the worker pool.
		#
		# @param [String]

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