require 'spec-helper'
require 'ostruct'

describe "Cartan::Node" do

  before(:each) do
    @config = Cartan::Config.new(
      :namespace => "cartan",
      :log_level => :info
    )
    @node = Cartan::Node.new(@config)
  end

  it "should be able to load the log" do
    @node.config[:namespace].should == "cartan"
  end

  it "should have a uuid" do
    @node.uuid.should be_kind_of(String)
  end

  it "should be able to start and stop the EM reactor" do
    @node.start do
      EM.reactor_running?.should be_true
      @node.stop
    end
    EM.reactor_running?.should be_false
  end

  it "should be able to transition between states" do
    @node.node_state.should == "idle"
    @node.start do
      @node.node_state.should == "running"
      @node.stop
    end
    @node.node_state.should == "idle"
  end

  it "should be able to capture interupt signals" do
    Cartan::Log.loggers.clear # Suppress noisy warnings

    @node.start do
      Process.kill("HUP", $$) 
    end
    @node.node_state.should == "idle"
  end

  it "should be able to provide personal info" do
    @node.info[:type].should == "Node"
  end

  it "should respond to heartbeat requests" do
    @node.start do
      myNode = OpenStruct.new
      myNode.message_received = false
      myNode.otherUUID = @node.uuid

      msg = Cartan::Messaging.new("123", @config[:namespace], @config[:amqp])
      msg.start

      handler = Cartan::MessageHandler.new(myNode, proc{ :ready }) do
        handle "heartbeat.response", :ready do |uuid, label, message|

          uuid.should == @node.otherUUID
          label.should == "heartbeat.response"
          message["type"].should == "Node"

          @node.message_received = true
        end
      end

      msg.handle_exclusive(handler)
      msg.send_node(@node.uuid, "heartbeat")     

      EM::Synchrony.sleep(0.5) 
      myNode.message_received.should be_true

      msg.stop
      @node.stop
    end
  end

end


