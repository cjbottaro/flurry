module Flurry
  module Processor

    def initialize(*args)
    end

    def initial_state
    end

    def done
    end

    def emit(message)
      @worker.emit_from_processor(message)
    end

    def before_begin_computation(*args)
    end

    def set_worker(worker)
      @worker = worker
    end

  end
end
