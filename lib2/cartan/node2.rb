module Cartan

	class Node

		attr_accessor :config_path, :uuid, :redis, :amqp, :channel
		state_machine :connection_state, :initial => :unconfigured do
			before_transition :unconfigured => :configured, :do => :configure
			before_transition :configured => :connected, :do => :connect
			before_transition :connected => :disconnected, :do => :disconnect

			event(:configure!) { transition :unconfigured => :configured }
			event(:connect!) { transition :configured => :connected }
			event(:disconnect!) { transition :connected => :disconnected }

			%w[ configured connected disconnected ].each do |s|
				after_transition any => s.to_sym, :do => "when_#{s}".to_sym
			end
		end

		def initialize(config_path)
			super() # Must call for the statemachine to work
			@config_path = config_path

			configure!
		end

		def configure
			load_config(@config_path)
			capture_signals
			@uuid = SecureRandom.uuid
		end

		def when_configured
			connect!
		end

		def connect
			connect_redis
			connect_amqp
		end

		def when_connected
		end

		def disconnect
			@redis.quit
			@amqp.close
			EM.stop
		end

		def when_disconnected
		end

		# Loads a config file.
		#
		# @param [String] The path to the configuration file.
		def load_config(config)
			Cartan::Config.from_file(config, "json")

			Cartan::Log.loggers << Logger.new(Cartan::Config[:log_location])
			Cartan::Log.level = Cartan::Config[:log_level]
		end

		# Opens a Redis connection
		def connect_redis
			@redis = Redis.connect Cartan::Config[:redis]

		# Opens an AMQP connection and channel
		def connect_amqp
			@amqp = EM::S::AMQP.connect Cartan::Config[:amqp]
			@channel = EM::S::AMQP::Channel.new(@amqp)
		end

		# Captures interupt signals 
		def capture_signals
			%w[ TERM INT QUIT HUP ].each do |signal|
				Signal.trap(signal) { 
					Cartan::Log.error "SIG#{signal} received, stopping."
					disconnect!
				}
			end
		end

	end
end
