module Flurry
  module Router
    class Random
      include Flurry::Router

      def route(message)
        @workers.sample.emit(message)
      end

    end
  end
end
