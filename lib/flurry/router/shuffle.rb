module Flurry
  module Router
    class Shuffle
      include Flurry::Router

      def initialize
        @index = 0
      end

      def route(message)
        @workers[ @index % @worker_count ].emit(message)
        @index += 1
      end

    end
  end
end
