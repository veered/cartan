module Cartan

	class Worker
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging

		attr_accessor :uuid, :amqp, :channel, :foreman, :resource, :work_queue_name, :work_queue

		state_machine :state, :initial => :initializing do
			after_transition [:initializing, :recovering] => :connecting, :do => :connect
			after_transition [:connecting, :working] => :idling, :do => :idle
			after_transition :idling => :configuring, :do => :configure
			after_transition :configuring => :working, :do => :work
			after_transition any => :recovering, :do => :recover
			after_transition [:idling, :recovering] => :disconnecting, :do => :disconnect

			event :initialized do
				transition :initializing => :connecting 
			end

			event :connected do
				transition :connecting => :idling 
			end

			event :configure do
				transition :idling => :configuring
			end

			event :work do
				transition :configuring => :working
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
			open_amqp_channel Cartan::Config[:amqp]

			get_foreman
			subscribe_crew(&method(:process_crew))
			subscribe_private(&method(:process_exclusive))
		end

		def connect_work
			@work_queue = @channel.queue @work_queue_name
			@work_queue.subscribe(&method(:process_work))
		end


		# Attempts to gracefully close all connections
		def disconnect
			@amqp.close { EM.stop }
		end

		def idle
			quit
		end

		def configure
			join
		end

		def work
			Cartan::Log.info "Working the night away!"
		end

		def process_crew(headers, payload)

		end

		def process_exclusive(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "foreman.heartbeat"
				send_message message["uuid"], "worker.heartbeat"
			when "foreman.assign"
				idle
				@resource = message["msg"]["resource"]
				configure

			when "resource.config"
				@work_queue_name = message["msg"]["work"]
				connect_work
				work
			end
		end

		def process_work(headers, payload)
			message = MP.unpack payload
		end

		# Declares the node to the foreman
		def declare(uuid)
			send_foreman("worker.declare", { :score => Cartan::Config[:score] })
		end

		def join
			send_message @resource, "worker.join"
		end

		def quit
			@worker_queue.unsubscribe unless @worker_queue.nil?

			send_message(@resource, "worker.quit") unless @resource.nil?
			@resource = nil
		end

	end

end