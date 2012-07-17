require 'helper'

# binding.pry

describe "Orchestrator" do
	
	before(:all) do
		options = {
			:rabbit => {
				:host => 'localhost',
				:port => 6372,
				:namespace => "cartan"
			},

			:redis => {
				:host => 'localhost',
				:port => 6379,
				:namespace => "cartan"
			},

			:logger => Logger.new(STDOUT),

			:base_dir => "/etc/cartan",
			:pid_dir => "/var/run/cartan",

			:inputs => [
				{
					:name => "test",
					:workers => 3,
					:bin => "echo",

					:options => {
						:hello => :there
					}
				}
			]

		}

		@orchestrator = Cartan::Orchestrator.new(options)
	end

	describe "#connect_rabbit" do
		it "should have a connection to RabbitMQ" do
			@orchestrator.instance_variable_get("@bunny").status == :connected
		end

		it "should have a queue named cartan::orchestra::test" do
			queue = @orchestrator.instance_variable_get("@bunny").queue("cartan::orchestra::test", :passive => true)
			not queue.nil?
		end

		it "should not have a queue named cartan::orchestra::sdfsdf234" do
			begin
				queue = @orchestrator.instance_variable_get("@bunny").queue("cartan::orchestra::sdfsdf234", :passive => true)
				false
			rescue Bunny::ForcedChannelCloseError
				true	
			end
		end
	end

	describe "#connect_redis" do
		it "should have connected to Redis" do
			@orchestrator.instance_variable_get("@redis").client.connection.connected?
		end
	end

	describe "#listen" do
		it "should generate PID files" do
			@orchestrator.listen
			not Dir[File.join(@orchestrator.options[:pid_dir], "worker_*")].empty?
		end
	end


end