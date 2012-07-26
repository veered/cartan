$:.unshift File.dirname(__FILE__)
require 'eventmachine'
require 'em-synchrony'

require "pry"
require "debugger"

module Cartan
	VERSION = "0.0.1"
end

require 'cartan/helper'
require 'cartan/log'
require 'cartan/config'

require 'cartan/node'
require 'cartan/dns'
