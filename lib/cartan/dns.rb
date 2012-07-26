require 'redis'

module Cartan

  class DNS < Node

    def run
      super() do
          msg.subscribe(dns, &method(:process_dns))

          yield if block_given?
      end
    end

    def process_dns(type, message)
      binding.pry
      case type
      when "dns.add.request"
        add_host(message["msg"]["host"], message["msg"]["host_uuid"])
        msg.send_node(message["uuid"], "dns.add.response", { "success" => "true" })
      when "dns.get.request"
        host_uuid = get_host(message["msg"]["host"])
        msg.send_node(message["uuid"], "dns.get.response", { "host_uuid" => host_uuid })
      end

    end

    def add_host(host, uuid)
      redis.hset("hosts", host, uuid)
    end

    def get_host(host)
      redis.hget("hosts", host)
    end

    def redis
      @redis ||= Redis.new(@config[:redis])
    end

    def dns
      @dns ||= msg.create_queue("dns")
    end

    def type
      "dns"
    end

  end

end