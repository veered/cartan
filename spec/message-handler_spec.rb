require 'spec-helper'
require 'cartan/message-handler'

describe "MessageHandler" do

  attr_accessor :message_received, :all_message

  before(:each) do
    @state = proc{ :default }
    @handler = Cartan::MessageHandler.new(self, @state)
  end

  it "should be able to set hooks on initialization" do
    @handler = Cartan::MessageHandler.new(self, proc{ :default }) do
      handle("", :default) {}
    end

    @handler.states.has_key?(:default).should be_true
  end

  it "should be able to set hooks on bind" do
    @handler = Cartan::MessageHandler.new(self, proc{ :default})
    @handler.bind do
      handle("", :default) {}
    end

    @handler.states.has_key?(:default).should be_true
  end

  it "should call its hooks" do
    node = OpenStruct.new
    node.message_received = false

    @handler = Cartan::MessageHandler.new(node, proc{ :default }) do
      handle("hello", :default) do |uuid, label, message|
        uuid.should == "2"
        label.should == "hello"
        message.should == "sup"

        @node.message_received = true
      end
    end

    @handler.receive("2", "hello", "sup")
    node.message_received.should be_true
  end

  it "should call the all hook" do
    node = OpenStruct.new
    node.message_received = false

    @handler = Cartan::MessageHandler.new(node, proc{ :default}) do
      handle(/car.*n/) do |uuid, label, message|
        uuid.should == "bleh"
        label.should == "cartcartan"
        message.should == { :meh => :meh }

        @node.message_received = true
      end
    end

    @handler.receive("bleh", "cartcartan", { :meh => :meh })
    node.message_received.should be_true
  end

  it "should reject invalid states" do

    lambda {
      @handler = Cartan::MessageHandler.new(self) do
        handle //, "asdf" do |uuid, label, message|

        end
      end
    }.should raise_error

  end

  it "should pass stress test #1" do
    state = :blimey

    node = OpenStruct.new
    node.message_received = false
    node.all_message = false

    @handler = Cartan::MessageHandler.new(node, proc{ state }) do

      handle(/.*/) do |uuid, label, message|
        @node.all_message = true
      end

      handle(/.*/, [:blimey, :arg]) do |uuid, label, message|
        uuid.should == "blimey"
        label.should == "blimey"
        message.should == "blimey"

        @node.message_received = true
      end

      handle(/.*/, [:dark, :night]) do |uuid, label, message|
        uuid.should == "dark"
        label.should == "dark"
        message.should == "dark"

        @node.message_received = true
      end
    end

    @handler.receive("blimey", "blimey", "blimey")
    node.message_received.should be_true
    node.all_message.should be_true

    node.message_received = false
    node.all_message = false
    state = :arg
    @handler.receive("blimey","blimey","blimey")
    node.message_received.should be_true
    node.all_message.should be_true

    node.message_received = false
    node.all_message = false
    state = :unknown
    @handler.receive("","","")
    node.message_received.should be_false
    node.all_message.should be_true

    node.message_received = false
    state = :dark
    @handler.receive("dark","dark","dark")
    node.message_received.should be_true
  end

end