require 'json'

module Cartan

	class Config < Hash

		# Loads a JSON configuration file
		def self.from_file(filename)
			config = Config.new
			config.merge!(JSON.parse(filename, :symbolize_names => true))
		end

		def initialize
			self[:log_level] = :info
			self[:log_location] = STDOUT

			self[:namespace] = "cartan"
			self[:redis] = {}
			self[:amqp] = {}
		end

	end

end