require "flurry/worker"

module Flurry
  class Component

    attr_reader :processor, :concurrency, :router, :workers
    private :processor, :concurrency

    def initialize(processor, concurrency, router)
      @processor = processor
      @concurrency = concurrency
      start_workers
      build_router(router)
    end

    def start_workers
      @workers = concurrency.times.map{ Worker.new(processor) }
    end

    def build_router(router_spec)
      @router = case router_spec
      when Router
        router_spec
      when Class
        router_spec.new
      when Array
        router_class, options = router_spec
        router_class.new(options)
      end

      @router.set_workers(workers)
    end

    def setup(routers, incoming_pids, args)
      workers.each do |worker|
        worker.call [:setup, routers, incoming_pids, args]
      end
    end

    def pids
      workers.map(&:pid)
    end

    def emit(message)
      router.route(message)
    end

    def end_computation
      router.broadcast([:done])
    end

    def worker_stats
      @workers.inject({}) do |memo, worker|
        memo[worker.inspect] = worker.call([:stats])
        memo
      end
    end

    def terminate
      @workers.each{ |worker| Process.kill("TERM", worker.pid) }
    end

  end
end
