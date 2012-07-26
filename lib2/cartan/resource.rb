module Cartan

	class Resource
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging

		attr_accessor :uuid, :amqp, :channel, :foreman

		state_machine :state, :initial => :initializing do
			after_transition [:initializing, :recovering] => :connecting, :do => :connect
			after_transition :connecting => :harvesting, :do => :harvest
			after_transition any => :recovering, :do => :recover
			after_transition [:harvesting, :recovering] => :disconnecting, :do => :disconnect

			event :initialized do
				transition :initializing => :connecting 
			end

			event :connected do
				transition :connecting => :harvesting 
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

			@work_queue_name = ns "work", @uuid

			EM.synchrony do
				initialized
			end
		end

		# Connects to RabbitMQ.
		def connect
			connect_amqp
			connect_work

			declare @uuid
			connected
		end

		# Connects to RabbitMQ
		def connect_amqp
			open_amqp_channel Cartan::Config[:amqp]

			get_foreman
			subscribe_crew(&method(:process_crew))
			subscribe_private(&method(:process_exclusive))
		end

		# Connects to work queue
		def connect_work
			@work = @channel.fanout @work_queue_name
			queue = @channel.queue @work_queue_name
			queue.bind @work
		end

		# Connects to Redis
		def connect_redis(config)
			@redis = Redis.connect config
		end

		# Attempts to gracefully close all connections
		def disconnect
			@redis.quit
			@amqp.close { EM.stop }
		end

		def harvest
			EM::S.add_periodic_timer(1) {
				send_work("resource.bogus", "Hello")
			}
		end

		def process_crew(headers, payload)

		end

		def process_exclusive(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "foreman.heartbeat"
				send_message message["uuid"], "resource.heartbeat"
			when "foreman.config"
				connect_redis message["msg"]["redis"]

			when "worker.join"
				send_message(message["uuid"], "resource.config", { 
					"work" => @work_queue_name,
					"tool" => ""
				})
			end
		end

		# Declares the node to the foreman
		def declare(uuid)
			send_foreman("resource.declare", { :score => Cartan::Config[:score] })
		end

		def send_work(type, msg="")
			@work.publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
											:type => type)
		end

	end

end