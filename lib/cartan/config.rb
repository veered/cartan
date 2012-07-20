require 'mixlib/config'
require 'json'

module Cartan

	class Config
		extend Mixlib::Config

		class << self
			alias_method :from_file_ruby, :from_file
		end

		def self.from_file_json(filename)
			self.from_stream_json(IO.read(filename))
		end

		def self.from_stream_json(input)
			configuration.merge!(JSON.parse(input, :symbolize_names => true))
		end

		def self.from_file(filename, parser="ruby")
			send("from_file_#{parser}".to_sym, filename)
		end

		log_level :info
		log_location STDOUT

	end

end