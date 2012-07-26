module Cartan

	class RabbitExchange

		ns = Cartan::ns

		def initialize(uuid, channel, exchange_name, opts = {})
			@opts = {
				:type => :fanout
			}.merge(opts)

			@uuid = uuid

			@channel = channel
			@exchange = @channel.exchange(ns exchange_name, @opts)
		end

		def send_message(type, message)
			@exchange.publish(MP.pack( { :uuid => @uuid, :msg => msg } ), 
												:type => type)
		end

	end

end

