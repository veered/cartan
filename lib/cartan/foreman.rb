require 'cartan/node/messaging'
require 'cartan/node/node-manager'
require 'cartan/node/redis'

module Cartan

  class Foreman < Cartan::Node
    attr_accessor :msg, :mng, :rd

   state_machine :runtime_state, :initial => :waiting do
      before_transition :waiting => :monitoring, :do => [:communicate, :monitor]
      before_transition :monitoring => :waiting, :do => :unmonitor
      before_transition :monitoring => :debugging, :do => :unmonitor
      before_transition :debugging => :monitoring, :do => :monitor

      event(:monitor!) { transition [:waiting, :debugging] => :monitoring , :if => :connected? }
      event(:wait!) { transition :monitoring => :waiting}
      event(:debug!) { transition :monitoring => :debugging}
    end 

    def node_loop
        Cartan::Log.info @uuid
 
        @msg = Cartan::Node::Messaging.new(@uuid, @config[:namespace], @config[:amqp]).open
        # @rd = Cartan::Node::Redis.new(@config[:namespace], @config[:redis])
        # @mng = Cartan::Node::NodeManager.new(@msg, @rd)

        monitor! 
    end

    def monitor
      Cartan::Log.info "Hello"
    end

  end

end