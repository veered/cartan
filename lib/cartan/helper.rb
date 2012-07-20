
EM::S = EM::Synchrony	
MP = MessagePack

class Hash
	def selectKeys(valid_keys)
		select do |key, value|
			valid_keys.include? key
		end
	end
end

module Cartan::Mixins

	module Node

		# Loads a config file.
		#
		# @param [String] The path to the configuration file.
		def load_config(config)
			Cartan::Config.from_file(config, "json")

			Cartan::Log.loggers << Logger.new(Cartan::Config[:log_location])
			Cartan::Log.level = Cartan::Config[:log_level]

			@uuid = Cartan::Config[:uuid] || SecureRandom.uuid
		end

		# Captures signals 
		def capture_signals
			%w[ TERM INT QUIT HUP ].each do |signal|
				Signal.trap(signal) { 
					Cartan::Log.error "SIG#{signal} received, stopping."
					killed 
				}
			end
		end

		# Namespaces the list of input arguments.
		#
		# @param [String] *args The parameters to namespace.
		def ns(*words)
			words.flatten.insert(0, Cartan::Config[:namespace].to_s).join(".")
		end

	end

	module Messaging

		# Opens an AMQP connection and channel
		#
		# @param [Hash] config The AMQP config to use.
		def open_amqp_channel(config)
			@amqp = EM::S::AMQP.connect config
			@channel = EM::S::AMQP::Channel.new(@amqp)
		end


		%w[ foreman crew ].each do |entity|
			line = __LINE__ + 2
			code = <<-EOF
				def get_#{entity}
					@#{entity} = @channel.fanout(ns "#{entity}")
				end

				def subscribe_#{entity}(&block)
					@#{entity} ||= get_#{entity}
					queue = @channel.queue(ns "#{entity}")
					queue.bind(ns "#{entity}")
					queue.subscribe { |args| block.call(args[0], args[1]) }
				end

				# Sends a message to the #{entity}
				# 
				# @param [String] type The type of the message to send
				# @param [String] msg The message to send
				def send_#{entity}(type, msg = "")		
					@#{entity}.publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
											:type => type)
				end
			EOF
			module_eval(code, __FILE__, line)
		end

		# Create and subscribe to private uuid queue.
		def subscribe_private(&block)
			queue = @channel.queue(ns("message", @uuid), :exclusive => true)
			queue.subscribe { |args| block.call(args[0], args[1]) }
		end

		# Sends a message to a node's private queue.
		#
		# @param [String] uuid The node's uuid
		# @param [String] type The type of the message to send
		# @param [String] msg The message to send
		def send_message(uuid, type, msg = "")
			@channel.default_exchange.
				publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
						:type => type,
						:routing_key => ns("message", uuid))
		end

	end

end