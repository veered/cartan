module Cartan

	class Foreman
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging, Cartan::Mixins::Redis

		attr_accessor :uuid, :redis, :amqp, :channel, :crew

		state_machine :state, :initial => :unitialized do
			before_transition :unitialized => :initialized, :do => [ :configure ]
			after_transition :unitialized => :initialized, :do => [ :connect ]

			before_transition any => :connected, :do => [ :connect_redis, :connect_amqp ]
			after_transition any => :connected, :do => [ :monitor ]

			before_transition any => :connected , :do => [ :]

			event :initialize do
				transition :unitialized => :initialized
			end

			event :connect do 
				transition any => :connected
			end

			event :monitor do
				transition [ :connected, :debugging ] => :monitoring
			end

			event :debug do
				transition :monitoring => :debugging
			end

			event :disconnect do
				transition [:connected, :monitoring] => :disconnected
			end
		end

		state_machine :state, :initial => :initializing do
			after_transition [:initializing, :recovering] => :connecting, :do => :connect
			after_transition [:connecting, :debugging] => :monitoring, :do => :monitor
			after_transition any => :recovering, :do => :recover
			after_transition [:monitoring, :recovering] => :disconnecting, :do => :disconnect

			event :initialized do
				transition :initializing => :connecting 
			end

			event :connected do
				transition :connecting => :monitoring 
			end

			event :

			event :debugged do
				transition :debugging => :monitoring
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
			@redis = Redis.connect Cartan::Config[:redis]
		end

		# Connects to RabbitMQ
		def connect_amqp
			open_amqp_channel Cartan::Config[:amqp]

			get_crew
			subscribe_foreman(&method(:process_foreman))
			subscribe_private(&method(:process_exclusive))
		end
		#region
		# Attempts to gracefully close connections
		def disconnect
			@redis.quit
			@amqp.close { EM.stop }
		end
		#endregion

		def monitor
			start_heartbeat
		end

		def debug
			# stop_heartbeat
		end

		# Process new message in foreman queue.
		#
		# @param [AMQP::Header] headers Headers (metadata) associated with this message (for example, routing key).
	    # @param [String] payload Message body (content). On Ruby 1.9, you may want to check or enforce content encoding.
		def process_foreman(headers, payload)
			message = MP.unpack(payload)

			case headers.type
			when "worker.declare"
				declare_worker message["uuid"], message["msg"]

			when "resource.declare"
				declare_resource message["uuid"], message["msg"]
				send_message message["uuid"], "foreman.config", { :redis => Cartan::Config[:redis] }

			when "command.debug"
				stop_heartbeats
				binding.pry
				heartbeats
			when "command.status"
				Cartan::Log.info ["",
					"Workers: #{@redis.zrange(workers, 0, -1)}", 
					"Resources: #{@redis.zrange(resources, 0, -1)}"
					].join("\n")
				
			end


		end

		# Process new message in the exclusive queue.
		#
		# @param [AMQP::Header] headers Headers (metadata) associated with this message (for example, routing key).
	    # @param [String] payload Message body (content). On Ruby 1.9, you may want to check or enforce content encoding.
	    def process_exclusive(headers, payload)
			message = MP.unpack(payload)

			case headers.type
			when /.*\.heartbeat/
				@redis.hset node_hash(message["uuid"]), "heartbeat", "received"
			end
	    end

		# Adds the worker to the pool of avaliable workers.
		#
		# @param [String] uuid The worker's uuid
		def declare_worker(uuid, metadata)
			@redis.hset node_hash(uuid), "resource", ""
			@redis.zadd(workers, metadata["score"], uuid)

			Cartan::Log.info "\nWorker declared."
		end

		# Adds the resource to the list of resources.
		#
		# @param [String] uuid The resource's uuid
		# @param [Hash] metadata The resource's metadata
		def declare_resource(uuid, metadata)
			@redis.zadd(resources, metadata["score"], uuid)

			Cartan::Log.info "\nResource declared."
		end

		# Removes a worker from the worker pool
		#
		# @param [String] The worker's uuid
		def remove_worker(uuid)
			resource = @redis.hget node_hash(uuid), "resource"
			@redis.srem resource_workers(resource), uuid unless resource.nil? 

			@redis.del node_hash(uuid)
			@redis.zrem workers, uuid
		end

		# Removes a resource from the resource pool
		#
		# @param [String] The resource's uuid
		def remove_resource(uuid)
			@redis.del node_hash(uuid)
			@redis.del resource_workers(uuid)
			@redis.zrem resources, uuid
		end

		# Send node's regular heartbeats to ensure continued continuity.
		def start_heartbeat
			flush_heartbeats(workers)
			flush_heartbeats(resources)

			@heartbeat = EM::S.add_periodic_timer(1) {
				check_heartbeat(rand_member(workers), 10) { |uuid, alive| remove_worker uuid unless alive }
				check_heartbeat(rand_member(resources), 10) { |uuid, alive| remove_resource uuid unless alive }
			}
		end

		def stop_heartbeat
			@heartbeat.cancel unless @heartbeat.nil?
			flush_heartbeats(workers)
			flush_heartbeats(resources)
		end

		# Clears all heartbeat requests
		def flush_heartbeats(nodes)
			nodes = @redis.zrange nodes, 0, -1
			nodes.each do |node|
				@redis.hset node_hash(node), "heartbeat", "received"
			end
		end

		# Checks the heartbeat of a node.
		#
		# @param [String] uuid The uuid of the node
		# @yield [uuid, alive] Executed after heartbeat timeout.
		def check_heartbeat(uuid, timeout, &block)
			return if uuid.nil? or @redis.hget(node_hash(uuid), "heartbeat") == "sent"

			send_message uuid, "foreman.heartbeat"
			@redis.hset node_hash(uuid), "heartbeat", "sent"

			EM::S.add_timer(timeout) { 
				alive = @redis.hget(node_hash(uuid), "heartbeat") == "received"
				block.call(uuid, alive)
			}
		end

		# Distributes available workers among needy resources
		def worker_allocation
			EM::S.add_periodic_timer(5) do
				free_workers = @redis.zrevrange(workers, 0, -1).select do |node|
					binding.pry
					@redis.hget(node_hash(node), "resource") == ""
				end
				# debugger
				free_workers.each do |worker|
					resource, score = highest_member(resources)
					break if resource.nil? or score <= 0
					# debugger
					assign_worker(worker, resource)
				end
			end
		end

		def assign_worker(worker, resource)
			@redis.hset node_hash(worker), "resource", resource			
			@redis.sadd resource_workers(resource), worker
			send_message worker, "foreman.assign", { :resource => resource }
		end


	    def workers; ns @workers; end
	    def resources; ns @resources; end
	    def resource_workers(uuid); ns uuid, "workers"; end
	    def node_hash(uuid); ns uuid; end

	end

end