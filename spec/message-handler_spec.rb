require 'spec-helper'
require 'cartan/message-handler'

describe "MessageHandler" do

  attr_accessor :message_received, :all_message

  before(:each) do
    @state = proc{ :default }
    @handler = Cartan::MessageHandler.new(self, @state)
  end

  it "should be able to set hooks on initialization" do
    @handler = Cartan::MessageHandler.new(self, proc{ :default}) do
      state(:default) {}
    end

    @handler.states.has_key?(:default).should be_true
  end

  it "should be able to set hooks on handle" do
    @handler = Cartan::MessageHandler.new(self, proc{ :default})
    @handler.handle do
      state(:default) {}
    end

    @handler.states.has_key?(:default).should be_true
  end

  it "should call its hooks" do
    self.message_received = false

    @handler = Cartan::MessageHandler.new(self, proc{ :default}) do
      state(:default) do |uuid, label, message|
        uuid.should == "2"
        label.should == "hello"
        message.should == "sup"

        @node.message_received = true
      end
    end

    @handler.receive("2", "hello", "sup")
    self.message_received.should be_true
  end

  it "should call the all hook" do
    self.message_received = false

    @handler = Cartan::MessageHandler.new(self, proc{ :default}) do
      state(all) do |uuid, label, message|
        uuid.should == "bleh"
        label.should == "cartcartan"
        message.should == { :meh => :meh }

        @node.message_received = true
      end
    end

    @handler.receive("bleh", "cartcartan", { :meh => :meh })
    self.message_received.should be_true
  end

  it "should pass stress test #1" do
    state = :blimey
    self.message_received = false
    self.all_message = false

    @handler = Cartan::MessageHandler.new(self, proc{ state }) do

      state(all) do |uuid, label, message|
        @node.all_message = true
      end

      state(:blimey) do |uuid, label, message|
        uuid.should == "blimey"
        label.should == "blimey"
        message.should == "blimey"

        @node.message_received = true
      end

      state(:dark) do |uuid, label, message|
        uuid.should == "dark"
        label.should == "dark"
        message.should == "dark"

        @node.message_received = true
      end
    end

    @handler.receive("blimey", "blimey", "blimey")
    self.message_received.should be_true
    self.all_message.should be_true

    self.message_received = false
    self.all_message = false
    state = :unknown
    @handler.receive("","","")
    self.message_received.should be_false
    self.all_message.should be_true

    self.message_received = false
    state = :dark
    @handler.receive("dark","dark","dark")
    self.message_received.should be_true
  end

end