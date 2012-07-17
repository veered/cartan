require 'json'
require 'bunny'
require 'redis-namespace'
require 'logger'
require 'fileutils'

module Cartan

	class Orchestrator
		attr_accessor :options

		def initialize(options)
			@options = {
				:logger => Logger.new(STDOUT),

				:base_dir => "/etc/cartan",
				:pid_dir => "/var/run/cartan"
			}.merge(options)

			connect_redis
			connect_rabbit
		end

		# Connects to RabbitMQ
		def connect_redis
			@redis = Redis.new @options[:redis].selectKeys [:host, :port]
			@redis.client.connect
			@rd = Redis::Namespace.new(@options[:redis][:namespace], :redis => @redis)
		end

		# Connects to RabbitMQ
		def connect_rabbit
			@bunny = Bunny.new @options[:rabbit].selectKeys [:host]
			@bunny.start

			@orchestra = @bunny.exchange("#{@options[:rabbit][:namespace]}::orchestra", :type => :topic)
			@options[:inputs].each do |input|
				queue = @bunny.queue("#{@options[:rabbit][:namespace]}::orchestra::#{input[:name]}")
				queue.bind(@orchestra, :key => input[:name])
			end
		end

		# Respawns workers for every input.	
		def listen
			Dir[File.join(@options[:pid_dir], "worker_*")].each { |f| File.delete(f) }

			@options[:inputs].each do |input|
				input[:workers].times { spawnWorker(input) }
			end
		end

		# Spawns a worker for the given input.
		def spawnWorker(input)
			command = "#{input[:bin]} start" 
			command << " -q #{@options[:rabbit][:namespace]}::orchestra::#{input[:name]}"
			command << " -c #{input[:options].to_json}"

			pid = spawn(command)

			File.open(File.join(@options[:pid_dir], "worker_" + pid.to_s), "w+") { |f| f.write(pid) }
		end


	end

end