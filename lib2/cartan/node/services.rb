module Cartan

	class Node::Services < Array

		# Starts all services
		def start
			each &:start
		end

		# Stops all services
		def stop
			each &:stop
		end

	end

end



