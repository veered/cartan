require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "Cartan::Node" do
	before(:each) do
		@node = Cartan::Node.new(Cartan::Config.new)
	end

	after(:each) do
		@node = nil
	end

	it "should be able to load the log" do
		c = Cartan::Config.new
		c[:log_level] = :info
		c[:log_location] = STDOUT

		@node = Cartan::Node.new(c)

		@node.config[:log_level].should eql(:info)
		@node.config[:log_location].should eql(STDOUT)
	end

  it "should be able to capture interrupt signals" do
    @node.run do
      Process.kill("HUP", $$)
    end

    EM.reactor_running?.should be_false
  end

  it "should have a uuid" do
    @node.uuid.should be_kind_of(String)
  end

	it "should be able to start and stop eventmachine" do
    @node.run do
  		EM.reactor_running?.should be_true

      @node.stop
    end

    EM.reactor_running?.should be_false
	end
	
end
