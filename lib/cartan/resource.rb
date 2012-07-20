module Cartan

	class Resource
		include Cartan::Mixins::Node, Cartan::Mixins::Messaging

		attr_accessor :uuid, :amqp, :channel, :orchestrator

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

			get_orchestrator
			subscribe_orchestra &method(:process_orchestra)
			subscribe_private &method(:process_exclusive)
		end

		# Connects to work queue
		def connect_work
			@work = @channel.fanout ns("resource", uuid)
			queue = @channel.queue ns("resource", uuid)
			queue.bind @work
		end

		# Attempts to gracefully close all connections
		def disconnect
			@amqp.close { EM.stop }
		end

		def harvesting
			EM::S.add_periodic_timer(1) {
				send_work("resource.bogus", "Hello")
			}
		end

		def process_orchestra(headers, payload)
			
		end

		def process_exclusive(headers, payload)
			message = MP.unpack(payload)
			Cartan::Log.info "\nType: #{headers.type}\nMessage: #{message}"

			case headers.type
			when "orchestrator.heartbeat"
				send_message message["uuid"], "resource.heartbeat"
			end
		end

		# Declares the node to the orchestrator
		#
		# @param [String] uuid The uuid of the node to declare
		def declare(uuid)
			send_orchestrator("resource.declare")
		end

		def send_work(type, msg="")
			@work.publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
											:type => type)
		end

	end

end