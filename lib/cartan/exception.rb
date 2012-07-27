module Cartan

  class Exception

    class InvalidState < RuntimeError; end
    class ReactorNotRunning < RuntimeError; end

  end

end