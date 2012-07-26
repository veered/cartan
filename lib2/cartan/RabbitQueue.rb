module Cartan

	class RabbitQueue
		attr_accessor :callbacks

		ns = Cartan::ns

		def initialize(channel, exchange_name = "", queue_name, opts = {})
			@opts = {
				exchange_type => :fanout,
				routing_key => ""
			}.merge(opts)

			@channel = channel

			if exchange_name == ""
				@exchange = @channel.exchange(ns(exchange_name), :type => @opts[:exchange_type])
				@queue = @channel.queue(ns(queue_name))
				@queue.bind(@exchange)
			else
				@queue = @channel.queue(ns("message", queue_name), :exclusive => true)
			end

			@queue.subscribe(&method(:process_messages))
			@callbacks = []
		end

		def process_messages(headers, payload)
			message = MP.unpack(payload)
			type = headers.type

			@callbacks.each do |hook, action|
				action.call(type, message) unless type.match(hook).nil?
			end
		end

		def add(hook, group = :default_group, &action)
			@callbacks << [hook, action, group]	
		end

		def add_group(callback_list, group = :default_group)
			@callback_list.each do |hook, action|
				add(hook, group, &action)
			end
		end

		def remove(group)
			@callbacks.delete_if { |_,_,g| g == group}
		end

		def remove_all
			@callbacks = []
		end

	end

end