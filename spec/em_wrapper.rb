require 'helper'

module Cartan

	def self.wrap(cls, hooks = [], &block)
		hooks = cls.state_machine.events.map(&:name) if hooks.empty?

		hooks.each do |hook|
			old_method = "_#{hook.to_s}".to_sym

			cls.class_eval {
				alias_method old_method, hook
				define_method(hook) do |*args|
					Fiber.yield self
					send(old_method, *args)
				end
			}

		end

		Fiber.new(&block) if block_given?
	end

	def self.unwrap(cls, hooks = [])
		hooks = cls.state_machine.events.map(&:name) if hooks.empty?

		hooks.each do |hook|
			old_method = "_#{hook.to_s}".to_sym
			cls.class_eval { alias_method hook, old_method }
		end
	end

end
