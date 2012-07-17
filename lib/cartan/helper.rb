require "cartan"

class Hash
	def selectKeys(valid_keys)
		select do |key, value|
			valid_keys.include? key
		end
	end
end

module Cartan

end