module Cartan::Foreman::Services

	class Heartbeat

		def initialize(foreman, nodes)
			@foreman = foreman
			@nodes = nodes

			@redis = foreman.redis
			@queue = foreman.exclusive
			@label = :"#{@nodes}_heartbeat"
		end

		def start
			flush_heartbeat nodes
			@queue.add(/#{nodes.chomp('s')}.heartbeat/, @label, &method(:heartbeat_received))
			@heartbeat = EM::S.add_periodic_timer(1) { check_heartbeat(rand_member(nodes), 10, &block) }
		end

		def stop
			@heartbeat.cancel
			@queue.remove :"#{@nodes}_heartbeat"
		end

		# Checks the heartbeat of a node.
		#
		# @param [String] uuid The uuid of the node
		# @yield [uuid, alive] Executed after heartbeat timeout.
		def check_heartbeat(uuid, timeout, &block)
			return if @redis.hget(hsh(uuid), "heartbeat") == "sent"

			send_message uuid, "foreman.heartbeat"
			@redis.hset hsh(uuid), "heartbeat", "sent"

			EM::S.add_timer(timeout) {block.call(uuid) if @redis.hget(hsh(uuid), "heartbeat") == "received"}
		end

		# Responds to a heartbeat message
		def heartbeat_received(type, message)
			@redis.hset hsh(message["uuid"]), "hearbeat", "received"
		end

		# Clears all heartbeat requests
		def flush_heartbeat(nodes)
			nodes = @redis.zrange nodes, 0, -1
			nodes.each do |node|
				@redis.hset hsh(node), "heartbeat", "received"
			end
		end

	end

end