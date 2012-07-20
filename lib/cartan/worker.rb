module Cartan

	class Worker
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging

		attr_accessor :uuid, :amqp, :channel, :orchestrator

		state_machine :state, :initial => :initializing do
			after_transition [:initializing, :recovering] => :connecting, :do => :connect
			after_transition [:connecting, :working] => :idling, :do => :idle
			after_transition :idling => :working, :do => :work
			after_transition any => :recovering, :do => :recover
			after_transition [:idling, :working, :recovering] => :disconnecting, :do => :disconnect

			event :initialized do
				transition :initializing => :connecting 
			end

			event :connected do
				transition :connecting => :idling 
			end

			event :work do
				transition :idling => :working
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

			get_orchestrator
			subscribe_orchestra &method(:process_orchestra)
			subscribe_private &method(:process_exclusive)
		end

		# Attempts to gracefully close all connections
		def disconnect
			@amqp.close { EM.stop }
		end

		def idle

		end

		def work

		end

		def process_orchestra(headers, payload)

		end

		def process_exclusive(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "orchestrator.heartbeat"
				send_message message["uuid"], "worker.heartbeat"
			end
		end

		# Declares the node to the orchestrator
		#
		# @param [String] uuid The uuid of the worker to declare
		def declare(uuid)
			send_orchestrator("worker.declare")
		end

	end

end