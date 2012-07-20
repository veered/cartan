module Cartan

	class Foreman
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging

		attr_accessor :uuid, :redis, :amqp, :channel, :crew

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

		# Initialize node.
		# 
		# @param [String] config The path to the configuration file.
		def initialize(config)
			super()

			load_config(config)
			capture_signals

			@workers = "workers"
			@resources = "resources"

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
			open_amqp_channel Cartan::Config[:amqp]

			get_crew
			subscribe_foreman &method(:process_foreman)
			subscribe_private &method(:process_exclusive)
		end

		# Attempts to gracefully close connections
		def disconnect
			@redis.unbind
			@amqp.close { EM.stop }
		end

		def monitor
			EM::S.add_periodic_timer(1) {
				worker = @redis.srandmember workers
				next if worker.nil?

				send_message worker, "foreman.heartbeat"
				@redis.hset node_hash(worker), "heartbeat", false

				EM::S.add_timer(5) { 
					heartbeat = @redis.hget(node_hash(worker), "heartbeat") == "true"
					remove_worker worker unless heartbeat
				}
			}
		end

		# Process new message in foreman queue.
		#
		# @param [AMQP::Header] headers Headers (metadata) associated with this message (for example, routing key).
	    # @param [String] payload Message body (content). On Ruby 1.9, you may want to check or enforce content encoding.
		def process_foreman(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "worker.declare"
				declare_worker message["uuid"]
				send_message(message["uuid"], "foreman.ack")

			when "resource.declare"
				declare_resource message["uuid"]
				send_message message["uuid"], "foreman.config", { :redis => Cartan::Config[:redis] }
			when "resource.request_workers"
				workers = get_workers message["workers"]
				send_message message["uuid"], "foreman.assign", { :workers => workers }
			end

		end

		# Process new message in the exclusive queue.
		#
		# @param [AMQP::Header] headers Headers (metadata) associated with this message (for example, routing key).
	    # @param [String] payload Message body (content). On Ruby 1.9, you may want to check or enforce content encoding.
	    def process_exclusive(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when /.*\.heartbeat/
				@redis.hset node_hash(message["uuid"]), "heartbeat", true
			end
	    end

		# Adds the worker to the pool of avaliable workers.
		#
		# @param [String] uuid The worker's uuid
		def declare_worker(uuid)
			@redis.sadd(workers, uuid)
		end

		# Adds the resource to the list of resources.
		#
		# @param [String] uuid The resource's uuid
		def declare_resource(uuid)
			@redis.sadd(resources, uuid)
		end

		# Removes a worker from the worker pool
		#
		# @param [String] The worker's uuid
		def remove_worker(uuid)
			@redis.del node_hash(uuid)
			@redis.srem workers, uuid
		end

		# Removes a resource from the resource pool
		#
		# @param [String] The resource's uuid
		def remove_resource(uuid)
			@redis.del node_hash(uuid)
			@redis.srem resources, uuid
		end

	    def workers; ns @workers; end
	    def resources; ns @resources; end
	    def node_hash(uuid); ns uuid; end

	end

end