require 'spec-helper'
require 'ostruct'

require 'cartan/messaging'

describe "Messaging" do

  MP = MessagePack

  before(:each) do
    @msg = Cartan::Messaging.new("123", "cartan")
  end

  describe "jn" do

    it "should leave one argument alone" do
      @msg.jn("hello").should == "hello"
    end

    it "should join multiple arguments" do
      @msg.jn("hello", "there", "buddy").should == "hello.there.buddy"
    end

    it "should accept an array" do
      @msg.jn(["hello", "there", "buddy"]).should == "hello.there.buddy"
    end

  end

  describe "ns" do

    it "should insert the namespace at the beginning" do
      @msg.ns("hello", "there").should == "cartan.hello.there"
    end

  end

  it "should be able to connect to and disconnect from amqp" do
    EM.synchrony do

      @msg.start
      @msg.amqp.should be_connected

      @msg.stop
      @msg.amqp.should_not be_connected

      EM.stop
    end
  end

  it "should be able to send messages" do
    
    sync do
      message_received = false

      queue = @msg.channel.queue(@msg.ns("foobar"), :auto_delete => true)
      queue.bind(@msg.exchange, :routing_key => @msg.ns("foobar"))

      queue.subscribe do |headers, payload|
        decoded = MP.unpack(payload)

        headers.type.should == "hola"
        decoded["uuid"].should == "123"
        decoded["msg"].should == "good day"

        message_received = true
      end

      @msg.send_message("foobar", "hola", "good day") 
      EM::Synchrony.sleep(0.5)

      message_received.should be_true
    end
  end

  it "should be able to subscribe to and unsubscribe from queues" do
    sync do

      node = OpenStruct.new
      node.message_received = false

      message_received = false
      handler = Cartan::MessageHandler.new(node, proc{ :default }) do
        state(:default) do |uuid, label, message|

          uuid.should == "123"
          label.should == "test"
          message.should == "test"

          @node.message_received = true
        end
      end

      @msg.subscribe("manny", handler)

      @msg.send_message("manny", "test", "test")
      EM::Synchrony.sleep(0.5)
      node.message_received.should be_true

      @msg.unsubscribe("manny")
      node.message_received = false

      @msg.send_message("manny", "haha", "sup")
      EM::Synchrony.sleep(0.5)
      node.message_received.should be_false
    end
  end

  def sync
    EM.synchrony do
      @msg.start
      yield
      @msg.stop
      EM.stop
    end
  end

end