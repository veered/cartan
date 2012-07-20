#!/usr/bin/env ruby
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'cartan'
require 'thor'

class CartanOrchestrator < Thor

	desc "start", "Start the orchestrator."
	method_option :config, :type => :string, :aliases => "-c",
				  :desc => "The path of the config file."
	def start
		orchestrator = Cartan::Orchestrator.new options[:config]
	end

end

CartanOrchestrator.start