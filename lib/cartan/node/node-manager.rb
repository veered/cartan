module Cartan

	class NodeManager
    attr_accessor :msg, :rd

    def initialize(messaging, redis)
      @msg = messaging
      @rd = redis
    end

    # Adds the worker to the pool of avaliable workers.
    #
    # @param [String] uuid The worker's uuid
    def declare_worker(uuid, metadata)
      @rd.sadd(workers, uuid)
    end

    # Adds the resource to the list of resources.
    #
    # @param [String] uuid The resource's uuid
    # @param [Hash] metadata The resource's metadata
    def declare_resource(uuid, metadata)
      @redis.zadd(resources, metadata["score"], uuid)
    end

    # Removes a worker from the worker pool
    #
    # @param [String] uuid The worker's uuid
    def remove_worker(uuid)
      @redis.del hsh(uuid)
      @redis.srem workers, uuid
    end

    # Removes a resource from the resource pool
    #
    # @param [String] uuid The resource's uuid
    def remove_resource(uuid)
      @redis.del hsh(uuid)
      @redis.zrem resources, uuid
    end

    private

      # Joins the words with a period.
      #
      # @param [String] *args The words to join together.
      def jn(*words)
        words.flatten.join(".")
      end

      # Retrieves the name of this node's Redis hash
      #
      # @param [String] node The uuid of the node to lookup.
      def hsh(node)
        jn "node", node
      end

	end

end