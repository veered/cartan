require 'state_machine'
require 'pry'

module Cartan

	class Foreman << Node

		attr_accessor :crew, :foreman, :exclusive, :services
		state_machine :runtime_state, :initial => :waiting do
			before_transition :waiting => :monitoring, :do => [:communicate, :monitor]
			before_transition :monitoring => :waiting, :do => :unmonitor
			before_transition :monitoring => :debugging, :do => :unmonitor
			before_transition :debugging => :monitoring, :do => :monitor

			event(:monitor!) { transition [:waiting, :debugging] => :monitoring , :if => :connected? }
			event(:wait!) { transition :monitoring => :waiting}
			event(:debug!) { transition :monitoring => :debugging}
		end

		def when_connected
			monitor!
		end

		# Setup communication channels
		def communicate
			@crew = RabbitExchange.new(@uuid, @channel, "crew")
			@foreman = RabbitQueue.new(@channel, "foreman", "foreman")
			@exclusive = RabbitQueue.new(@channel, "", @uuid)
		end

		# Brings up all monitoring
		def monitor
			@services = []
			heartbeats

			@services.each { |s| s.start }
		end

		def unmonitor
			unheartbeats
		end

		def heartbeats
			@services << Foreman::Services::Heartbeat.new(self, workers)
			@services << Foreman::Services::Heartbeat.new(self, resources)
		end

	end

end