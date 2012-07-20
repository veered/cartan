require 'helper'
require 'em_wrapper'
require 'json'

describe "Orchestrator" do
	config_path = "config.json"

	define_method(:wrap) do |*hooks|
		load('cartan/orchestrator.rb')
		@fiber = Cartan::wrap(Cartan::Orchestrator, hooks) { @orchestrator = Cartan::Orchestrator.new(config_path) }
		@orchestrator = @fiber.resume
	end

	define_method(:unwrap) do |*hooks|
		Cartan::unwrap(Cartan::Orchestrator, hooks)
		@orchestrator.fire_state_event(:killed)
	end
	
	before(:all) do
		config = {
			:special_flag => true,

			:orchestrator => {
				:namespace => :cartan,
				:amqp => {
					:host => "localhost"
				},
				
			}
		}

		File.open config_path, "w" do |file|
			file.write config.to_json
		end
	end

	after(:all) do
		File.delete(config_path)
	end

	describe "#initialized" do
 		before(:all) { wrap(:initialized) }
 		after(:all) { unwrap(:initialized) }

		it "should have loaded a config file" do
			Cartan::Config[:special_flag].should be true
		end
	end

	describe "#connected" do
		before(:all) { wrap(:connected) }
		after(:all) { unwrap(:connected) }

		it "should have a connection to RabbitMQ" do
			@orchestrator.instance_variable_get("@connection").should be_connected
		end

		it "should have connected to Redis" do
			@orchestrator.instance_variable_get("@redis").client.connection.connected?
		end
	end

end