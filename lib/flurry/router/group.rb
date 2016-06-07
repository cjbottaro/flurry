require "digest"

module Flurry
  module Router
    class Group
      include Flurry::Router

      def route(message)
        @workers[ routing_key(message).to_s.hash % @worker_count ].emit(message)
      end

      def routing_key(message)
        message
      end

    end
  end
end
