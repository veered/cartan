
EM::S = EM::Synchrony	
MP = MessagePack

class Hash
	def selectKeys(valid_keys)
		select do |key, value|
			valid_keys.include? key
		end
	end
end

# Monkey patch subscribe to start a new Fiber for each message handle
module EventMachine::Synchrony::AMQP
	class Queue
		def subscribe &block
            asubscribe { |headers, payload| Fiber.new { block.call(headers, payload) }.resume }
        end
    end
end



module Cartan

	# Namespaces the list of input arguments.
	#
	# @param [String] *args The parameters to namespace.
	def ns(*words)
		words.flatten.insert(0, Cartan::Config[:namespace].to_s).join(".")
	end

	module Mixins

		module Node

			# Loads a config file.
			#
			# @param [String] The path to the configuration file.
			def load_config(config)
				Cartan::Config.from_file(config, "json")

				Cartan::Log.loggers << Logger.new(Cartan::Config[:log_location])
				Cartan::Log.level = Cartan::Config[:log_level]

				@uuid = Cartan::Config[:uuid] || SecureRandom.uuid
			ed

			# Generates a uuid
			def generate_uuid
				SecureRandom.uuid
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

			def connect_redis
				@redis = Redis.connect Cartan::Config[:redis]

			# Opens an AMQP connection and channel
			#
			# @param [Hash] config The AMQP config to use.
			def connect_amqp
				@amqp = EM::S::AMQP.connect Cartan::Config[:amqp]
				@channel = EM::S::AMQP::Channel.new(@amqp)
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
						queue.subscribe &block
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
				queue.subscribe &block
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

			def file_to_message(file)
				file.read
			end

			def message_to_file(file, msg)
				file.write msg
			end

		end

		module Redis

			# Returns a random member from a sorted set.
			def rand_member(key)
				count = @redis.zcard key
				index = count == 0 ? 0 : Random.rand(count)
				@redis.zrange(key, index, index).first
			end

			# Returns the member with the highest score from a sorted set and the corresponding score.
			def highest_member(key)
				resource, score = @redis.zrevrange(key, 0, 0, :with_scores => true).first
			end

		end

	end

end