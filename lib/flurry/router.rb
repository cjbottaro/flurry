module Flurry
  module Router

    def set_workers(workers)
      @workers = workers
      @worker_count = workers.length
    end

    def broadcast(message)
      @workers.each{ |worker| worker.call(message) }
    end

  end
end

require "flurry/router/random"
require "flurry/router/shuffle"
require "flurry/router/group"
