require 'json'

module Cartan

  class Config < Hash

    # Loads a JSON configuration file
    #
    # @param [String] filename The name of the file to load.
    def self.from_file(filename)
      config = Config.new
      config.merge!(JSON.parse(filename, :symbolize_names => true))
    end

    # Set config defaults
    def initialize
      self[:log_level] = :info
      self[:log_location] = STDOUT

      self[:namespace] = "cartan"
      self[:redis] = {}
      self[:amqp] = {}
    end

  end

end