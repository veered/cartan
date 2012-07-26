require 'logger'
require 'mixlib/log'

module Cartan

  class Log
    extend Mixlib::Log

    # Do NOT log to STDOUT by default
    init nil

    # Monkeypatch Formatter to allow local show_time updates.
    class Formatter
      # Allow enabling and disabling of time with a singleton.
      def self.show_time=(*args)
        Mixlib::Log::Formatter.show_time = *args
      end
    end

  end
end