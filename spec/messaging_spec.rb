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

  it "should throw an error if the EM reactor isn't running" do
    lambda {
      @msg.start
    }.should raise_error
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

  it "should be able to add and remove handlers to queues and deal with unsubscriptions." do
    sync do

      node = OpenStruct.new
      node.message_received = false

      handler = Cartan::MessageHandler.new(node, proc{ :default }) do
        handle(/.*/, :default) do |uuid, label, message|

          uuid.should == "123"
          label.should == "test"
          message.should == "test"

          @node.message_received = true
        end
      end

      @msg.add_handler("manny", handler)

      @msg.send_message("manny", "test", "test")
      EM::Synchrony.sleep(0.5)
      node.message_received.should be_true

      @msg.remove_handler("manny", handler)
      node.message_received = false

      @msg.send_message("manny", "haha", "sup")
      EM::Synchrony.sleep(0.5)
      node.message_received.should be_false

      @msg.add_handler("manny", handler)
      @msg.unsubscribe("manny")
      node.message_received = false

      @msg.send_message("manny", "haha", "sup")
      EM::Synchrony.sleep(0.5)
      node.message_received.should be_false
    end
  end

  it "should be able to add multiple handlers" do
    sync do

      node = OpenStruct.new
      node.responses = []

      handlers = []
      10.times do
        handlers << Cartan::MessageHandler.new(node, proc{ :running }) do
          handle(/heartbeat/, :running) do |uuid, label, message|

            uuid.should == "123"
            label.should == "heartbeat"
            message.should == "test"

            @node.responses << true
          end
        end

        @msg.add_handler("darling", handlers.last)
      end

      @msg.send_message("darling", "heartbeat", "test")
      EM::Synchrony.sleep(0.5)
      node.responses.size.should == 10
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